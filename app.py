from flask import Flask, request, jsonify, send_from_directory
import os, openpyxl, re
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__, static_folder='static')

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:Qgsistemas2026!@db.oivclhmawflchbkusaab.supabase.co:5432/postgres')
WAREHOUSES = ['culiacan', 'cdmx', 'oax']
MIN_METROS_VENDIBLE = 100

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
    # Tabla de rollos individuales — se regenera con cada carga del Excel
    cur.execute('''
        CREATE TABLE IF NOT EXISTS rollos (
            id SERIAL PRIMARY KEY,
            warehouse TEXT NOT NULL,
            report_date TEXT NOT NULL,
            product_name TEXT NOT NULL,
            metros REAL NOT NULL,
            tipo TEXT NOT NULL,
            vendible BOOLEAN,
            cat TEXT DEFAULT 'Plastico'
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()

init_db()

def parse_restos(val):
    """Convierte '97+31' o '51+47+49' en lista de floats [97, 31] / [51, 47, 49]"""
    if val is None:
        return []
    s = str(val).replace(' ', '')
    parts = re.split(r'[+,]', s)
    result = []
    for p in parts:
        p = p.strip()
        if p:
            try:
                result.append(float(p))
            except:
                pass
    return result

def get_cat(name):
    nl = name.lower()
    if 'rafia' in nl: return 'Rafia'
    if 'transparente' in nl: return 'Transparente'
    if 'difus' in nl: return 'Difusado'
    if 'blanco' in nl: return 'Blanco'
    if 'greenpro' in nl: return 'Greenpro'
    if 'mulch' in nl or 'acolchado' in nl: return 'Mulch'
    if 'semilla' in nl or 'seeds' in nl: return 'Semilla'
    if 'nuf' in nl or 'water' in nl: return 'NUF'
    if 'prolong' in nl or 'bag' in nl: return 'Poscosecha'
    return 'Otros'

def is_plastic(name):
    """¿Es un producto de plástico con rollos medidos en metros?"""
    nl = name.lower()
    keywords = ['transparente', 'difus', 'blanco', 'greenpro', '303', 'difuso']
    return any(k in nl for k in keywords)

def parse_excel(file_stream):
    wb = openpyxl.load_workbook(file_stream, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))

    # Fecha del reporte
    title = str(rows[0][0] if rows else '')
    report_date = datetime.now().strftime('%d/%m/%Y')
    m = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', title, re.IGNORECASE)
    if m:
        months = {'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
                  'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12}
        day, month_str, year = m.group(1), m.group(2).lower(), m.group(3)
        if month_str in months:
            report_date = f"{int(day):02d}/{months[month_str]:02d}/{year}"

    products = {}   # name -> {total, cat}
    rollos = []     # [{product_name, metros, tipo, cat}]

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

        cat = get_cat(name)
        products[name] = {'total': total, 'cat': cat}

        # Para plásticos: extraer rollos individuales
        if is_plastic(name):
            # Jumbos: columnas 1-23 (cada valor numérico = 1 rollo)
            for col_idx in range(1, 24):
                if col_idx < len(row):
                    v = row[col_idx]
                    if v is not None and str(v).strip() not in ('', 'None', ' '):
                        try:
                            metros = float(str(v).replace(',',''))
                            if metros > 0:
                                rollos.append({
                                    'product_name': name,
                                    'metros': metros,
                                    'tipo': 'jumbo',
                                    'cat': cat
                                })
                        except: pass

            # Restos: columna 28 (formato '97+31' o número)
            if len(row) > 28:
                restos_raw = row[28]
                for metros in parse_restos(restos_raw):
                    if metros > 0:
                        rollos.append({
                            'product_name': name,
                            'metros': metros,
                            'tipo': 'restante',
                            'cat': cat
                        })

            # BI-COLOR/DAÑADO: columna 29 — marcamos como dañado
            if len(row) > 29 and row[29] is not None:
                for metros in parse_restos(row[29]):
                    if metros > 0:
                        rollos.append({
                            'product_name': name,
                            'metros': metros,
                            'tipo': 'danado',
                            'cat': cat
                        })

    return products, rollos, report_date


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400

    warehouse = request.form.get('warehouse', 'culiacan')
    if warehouse not in WAREHOUSES:
        return jsonify({'error': 'Invalid warehouse'}), 400

    f = request.files['file']
    try:
        parsed, rollos, report_date = parse_excel(f.stream)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    conn = get_conn()
    cur = conn.cursor()

    # Actualizar productos y snapshots (igual que antes)
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

    # Reemplazar rollos de este almacén y fecha
    cur.execute('DELETE FROM rollos WHERE warehouse = %s AND report_date = %s', (warehouse, report_date))
    for r in rollos:
        vendible = r['metros'] >= MIN_METROS_VENDIBLE and r['tipo'] != 'danado'
        cur.execute('''
            INSERT INTO rollos (warehouse, report_date, product_name, metros, tipo, vendible, cat)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (warehouse, report_date, r['product_name'], r['metros'], r['tipo'], vendible, r['cat']))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        'ok': True,
        'date': report_date,
        'products': len(parsed),
        'rollos': len(rollos)
    })

@app.route('/api/dashboard')
def dashboard():
    warehouse = request.args.get('warehouse', 'culiacan')
    if warehouse not in WAREHOUSES:
        return jsonify({'error': 'Invalid warehouse'}), 400

    conn = get_conn()
    cur = conn.cursor()

    cur.execute('SELECT DISTINCT report_date FROM snapshots WHERE warehouse = %s', (warehouse,))
    dates_rows = cur.fetchall()
    if not dates_rows:
        cur.close(); conn.close()
        return jsonify({'products': [], 'has_data': False})

    def parse_date(d):
        try:
            parts = d.split('/')
            return (int(parts[2]), int(parts[1]), int(parts[0]))
        except: return (0,0,0)

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
        curr_snap = {r['product_name']: r['total'] for r in cur.fetchall()}
        for name in curr_snap:
            if name in prev and prev[name] > curr_snap[name]:
                consumed = prev[name] - curr_snap[name]
                if name not in avg_consumption: avg_consumption[name] = []
                avg_consumption[name].append(consumed)

    cur.close(); conn.close()

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
        result.append({
            'name': name, 'cat': meta.get('cat', 'Otros'), 'total': total,
            'min_stock': meta.get('min_stock', 0),
            'avg_daily': round(avg, 1) if avg else None,
            'days': days, 'status': status, 'history_days': len(hist)
        })

    order = {'urgente': 0, 'pronto': 1, 'ok': 2, 'sin_datos': 3}
    result.sort(key=lambda x: order.get(x['status'], 3))
    return jsonify({'products': result, 'has_data': True, 'latest_date': latest_date, 'total_days_history': len(sorted_dates)})

@app.route('/api/rollos')
def get_rollos():
    warehouse = request.args.get('warehouse', 'culiacan')
    product = request.args.get('product', None)
    tipo = request.args.get('tipo', None)  # jumbo / restante / danado

    conn = get_conn()
    cur = conn.cursor()

    # Fecha más reciente con rollos
    cur.execute('SELECT MAX(report_date) as d FROM rollos WHERE warehouse = %s', (warehouse,))
    row = cur.fetchone()
    latest = row['d'] if row else None

    if not latest:
        cur.close(); conn.close()
        return jsonify({'rollos': [], 'resumen': [], 'report_date': None})

    query = 'SELECT * FROM rollos WHERE warehouse = %s AND report_date = %s'
    params = [warehouse, latest]
    if product:
        query += ' AND product_name = %s'; params.append(product)
    if tipo:
        query += ' AND tipo = %s'; params.append(tipo)
    query += ' ORDER BY product_name, tipo, metros DESC'

    cur.execute(query, params)
    rollos = [dict(r) for r in cur.fetchall()]

    # Resumen por producto
    cur.execute('''
        SELECT
            product_name,
            cat,
            COUNT(*) AS total_rollos,
            COUNT(*) FILTER (WHERE tipo = 'jumbo') AS rollos_jumbo,
            COUNT(*) FILTER (WHERE tipo = 'restante') AS rollos_restante,
            COUNT(*) FILTER (WHERE tipo = 'danado') AS rollos_danado,
            SUM(metros) AS metros_total,
            SUM(metros) FILTER (WHERE vendible = true) AS metros_vendibles,
            COUNT(*) FILTER (WHERE vendible = true) AS rollos_vendibles,
            COUNT(*) FILTER (WHERE vendible = false) AS rollos_no_vendibles
        FROM rollos
        WHERE warehouse = %s AND report_date = %s
        GROUP BY product_name, cat
        ORDER BY cat, product_name
    ''', (warehouse, latest))
    resumen = [dict(r) for r in cur.fetchall()]

    cur.close(); conn.close()

    return jsonify({
        'rollos': rollos,
        'resumen': resumen,
        'report_date': latest,
        'min_metros_vendible': MIN_METROS_VENDIBLE
    })

@app.route('/api/min_stock', methods=['POST'])
def set_min_stock():
    body = request.json
    warehouse = body.get('warehouse', 'culiacan')
    name = body.get('name')
    value = body.get('value', 0)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('UPDATE products SET min_stock = %s WHERE warehouse = %s AND name = %s',
                (float(value) or 0, warehouse, name))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({'ok': True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
