from flask import Flask, request, jsonify, send_from_directory, session, redirect
import json, os, openpyxl
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__, static_folder='static')
app.secret_key = 'qg_inv_secret_2026'

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres.oivclhmawflchbkusaab:Qgsistemas2026!@aws-1-eu-central-1.pooler.supabase.com:6543/postgres')
APP_PASSWORD = os.environ.get('APP_PASSWORD', 'QGinventario2026')

WAREHOUSES = ['culiacan', 'cdmx', 'oax']

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            warehouse TEXT NOT NULL,
            name TEXT NOT NULL,
            cat TEXT DEFAULT 'Otros',
            min_stock REAL DEFAULT 0,
            UNIQUE(warehouse, name)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id SERIAL PRIMARY KEY,
            warehouse TEXT NOT NULL,
            report_date TEXT NOT NULL,
            product_name TEXT NOT NULL,
            total REAL NOT NULL,
            UNIQUE(warehouse, report_date, product_name)
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()

init_db()

def logged_in():
    return session.get('auth') == True

def parse_excel(file_stream):
    wb = openpyxl.load_workbook(file_stream, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    title = str(rows[0][0] if rows else '')
    report_date = datetime.now().strftime('%d/%m/%Y')
    import re
    m = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', title, re.IGNORECASE)
    if m:
        months = {'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
                  'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12}
        day, month_str, year = m.group(1), m.group(2).lower(), m.group(3)
        if month_str in months:
            report_date = f"{int(day):02d}/{months[month_str]:02d}/{year}"
    products = {}
    for i, row in enumerate(rows):
        if i < 2: continue
        name = str(row[0] or '').strip()
        if not name or 'TOTAL' in name.upper(): continue
        total = 0
        for val in reversed(row):
            if val is not None and str(val).strip() not in ('', 'None'):
                try:
                    total = float(str(val).replace(',',''))
                    break
                except: pass
        cat = 'Otros'
        nl = name.lower()
        if 'rafia' in nl: cat = 'Rafia'
        elif 'transparente' in nl: cat = 'Transparente'
        elif 'difus' in nl: cat = 'Difusado'
        elif 'blanco' in nl: cat = 'Blanco'
        elif 'greenpro' in nl: cat = 'Greenpro'
        elif 'mulch' in nl or 'acolchado' in nl: cat = 'Mulch'
        elif 'semilla' in nl: cat = 'Semilla'
        products[name] = {'total': total, 'cat': cat}
    return products, report_date

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        pwd = request.json.get('password', '')
        if pwd == APP_PASSWORD:
            session['auth'] = True
            return jsonify({'ok': True})
        return jsonify({'ok': False}), 401
    return send_from_directory('static', 'login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

@app.route('/')
def index():
    if not logged_in():
        return redirect('/login')
    return send_from_directory('static', 'index.html')

@app.route('/api/status')
def status():
    if not logged_in():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_conn()
    cur = conn.cursor()
    result = {}
    for wh in WAREHOUSES:
        cur.execute('SELECT report_date FROM snapshots WHERE warehouse = %s ORDER BY report_date DESC LIMIT 1', (wh,))
        row = cur.fetchone()
        result[wh] = row['report_date'] if row else None
    cur.close()
    conn.close()
    return jsonify(result)

@app.route('/api/upload', methods=['POST'])
def upload():
    if not logged_in():
        return jsonify({'error': 'Unauthorized'}), 401
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400
    warehouse = request.form.get('warehouse', 'culiacan')
    if warehouse not in WAREHOUSES:
        return jsonify({'error': 'Invalid warehouse'}), 400
    f = request.files['file']
    try:
        parsed, report_date = parse_excel(f.stream)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    conn = get_conn()
    cur = conn.cursor()
    for name, info in parsed.items():
        cur.execute('''
            INSERT INTO products (warehouse, name, cat, min_stock)
            VALUES (%s, %s, %s, 0)
            ON CONFLICT (warehouse, name) DO UPDATE SET cat = EXCLUDED.cat
        ''', (warehouse, name, info['cat']))
        cur.execute('''
            INSERT INTO snapshots (warehouse, report_date, product_name, total)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (warehouse, report_date, product_name) DO UPDATE SET total = EXCLUDED.total
        ''', (warehouse, report_date, name, info['total']))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'ok': True, 'date': report_date, 'count': len(parsed)})

@app.route('/api/dashboard')
def dashboard():
    if not logged_in():
        return jsonify({'error': 'Unauthorized'}), 401
    warehouse = request.args.get('warehouse', 'culiacan')
    if warehouse not in WAREHOUSES:
        return jsonify({'error': 'Invalid warehouse'}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT report_date FROM snapshots WHERE warehouse = %s', (warehouse,))
    dates_rows = cur.fetchall()
    if not dates_rows:
        cur.close()
        conn.close()
        return jsonify({'products': [], 'has_data': False})
    def parse_date(d):
        try:
            parts = d.split('/')
            return (int(parts[2]), int(parts[1]), int(parts[0]))
        except:
            return (0,0,0)
    sorted_dates = sorted([r['report_date'] for r in dates_rows], key=parse_date)
    latest_date = sorted_dates[-1]
    cur.execute('SELECT product_name, total FROM snapshots WHERE warehouse = %s AND report_date = %s', (warehouse, latest_date))
    latest = {r['product_name']: r['total'] for r in cur.fetchall()}
    cur.execute('SELECT name, cat, min_stock FROM products WHERE warehouse = %s', (warehouse,))
    products_meta = {r['name']: {'cat': r['cat'], 'min_stock': r['min_stock']} for r in cur.fetchall()}
    avg_consumption = {}
    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i-1]
        curr_date = sorted_dates[i]
        cur.execute('SELECT product_name, total FROM snapshots WHERE warehouse = %s AND report_date = %s', (warehouse, prev_date))
        prev = {r['product_name']: r['total'] for r in cur.fetchall()}
        cur.execute('SELECT product_name, total FROM snapshots WHERE warehouse = %s AND report_date = %s', (warehouse, curr_date))
        curr = {r['product_name']: r['total'] for r in cur.fetchall()}
        for name in curr:
            if name in prev and prev[name] > curr[name]:
                consumed = prev[name] - curr[name]
                if name not in avg_consumption:
                    avg_consumption[name] = []
                avg_consumption[name].append(consumed)
    cur.close()
    conn.close()
    result = []
    for name, total in latest.items():
        meta = products_meta.get(name, {'cat': 'Otros', 'min_stock': 0})
        hist = avg_consumption.get(name, [])
        avg = sum(hist) / len(hist) if hist else None
        days = int(total / avg) if avg and avg > 0 else None
        if days is None: status = 'sin_datos'
        elif days <= 90: status = 'urgente'
        elif days <= 120: status = 'pronto'
        else: status = 'ok'
        result.append({'name': name, 'cat': meta.get('cat', 'Otros'), 'total': total, 'min_stock': meta.get('min_stock', 0), 'avg_daily': round(avg, 1) if avg else None, 'days': days, 'status': status, 'history_days': len(hist)})
    order = {'urgente': 0, 'pronto': 1, 'ok': 2, 'sin_datos': 3}
    result.sort(key=lambda x: order.get(x['status'], 3))
    return jsonify({'products': result, 'has_data': True, 'latest_date': latest_date, 'total_days_history': len(sorted_dates)})

@app.route('/api/min_stock', methods=['POST'])
def set_min_stock():
    if not logged_in():
        return jsonify({'error': 'Unauthorized'}), 401
    body = request.json
    warehouse = body.get('warehouse', 'culiacan')
    name = body.get('name')
    value = body.get('value', 0)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('UPDATE products SET min_stock = %s WHERE warehouse = %s AND name = %s', (float(value) or 0, warehouse, name))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'ok': True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
