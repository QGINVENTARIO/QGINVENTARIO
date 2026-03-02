from flask import Flask, request, jsonify, send_from_directory
import json, os, openpyxl
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__, static_folder='static')

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:Qgsistemas2026!@db.oivclhmawflchbkusaab.supabase.co:5432/postgres')

WAREHOUSES = ['culiacan', 'cdmx', 'oax']
MIN_METROS_VENDIBLE = 100  # metros mínimos para que un rollo sea vendible

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Tabla de productos (catálogo general, no plásticos)
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

    # Tabla de snapshots (para todos los productos — total en metros/kg/unidades)
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

    # NUEVA: tabla de rollos individuales de plástico
    cur.execute('''
        CREATE TABLE IF NOT EXISTS rollos (
            id SERIAL PRIMARY KEY,
            warehouse TEXT NOT NULL,
            product_name TEXT NOT NULL,
            metros REAL NOT NULL,
            tipo TEXT NOT NULL DEFAULT 'jumbo',  -- 'jumbo' o 'restante'
            vendible BOOLEAN GENERATED ALWAYS AS (metros >= 100) STORED,
            fecha_registro TIMESTAMP DEFAULT NOW(),
            notas TEXT
        )
    ''')

    conn.commit()
    cur.close()
    conn.close()

init_db()

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

# ─── Rutas existentes ───────────────────────────────────────────────────────

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
        curr_snap = {r['product_name']: r['total'] for r in cur.fetchall()}

        for name in curr_snap:
            if name in prev and prev[name] > curr_snap[name]:
                consumed = prev[name] - curr_snap[name]
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

        if days is None:
            status = 'sin_datos'
        elif days <= 90:
            status = 'urgente'
        elif days <= 120:
            status = 'pronto'
        else:
            status = 'ok'

        result.append({
            'name': name,
            'cat': meta.get('cat', 'Otros'),
            'total': total,
            'min_stock': meta.get('min_stock', 0),
            'avg_daily': round(avg, 1) if avg else None,
            'days': days,
            'status': status,
            'history_days': len(hist)
        })

    order = {'urgente': 0, 'pronto': 1, 'ok': 2, 'sin_datos': 3}
    result.sort(key=lambda x: order.get(x['status'], 3))

    return jsonify({
        'products': result,
        'has_data': True,
        'latest_date': latest_date,
        'total_days_history': len(sorted_dates)
    })

@app.route('/api/min_stock', methods=['POST'])
def set_min_stock():
    body = request.json
    warehouse = body.get('warehouse', 'culiacan')
    name = body.get('name')
    value = body.get('value', 0)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute('''
        UPDATE products SET min_stock = %s WHERE warehouse = %s AND name = %s
    ''', (float(value) or 0, warehouse, name))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({'ok': True})

# ─── NUEVAS RUTAS: Rollos individuales ──────────────────────────────────────

@app.route('/api/rollos', methods=['GET'])
def get_rollos():
    """Lista todos los rollos de un almacén, opcionalmente filtrado por producto."""
    warehouse = request.args.get('warehouse', 'culiacan')
    product = request.args.get('product', None)

    conn = get_conn()
    cur = conn.cursor()

    if product:
        cur.execute('''
            SELECT id, warehouse, product_name, metros, tipo, vendible, fecha_registro, notas
            FROM rollos
            WHERE warehouse = %s AND product_name = %s
            ORDER BY tipo DESC, metros DESC
        ''', (warehouse, product))
    else:
        cur.execute('''
            SELECT id, warehouse, product_name, metros, tipo, vendible, fecha_registro, notas
            FROM rollos
            WHERE warehouse = %s
            ORDER BY product_name, tipo DESC, metros DESC
        ''', (warehouse,))

    rollos = cur.fetchall()

    # Resumen por producto: jumbo, restantes, total metros, metros vendibles
    cur.execute('''
        SELECT
            product_name,
            COUNT(*) AS total_rollos,
            COUNT(*) FILTER (WHERE tipo = 'jumbo') AS rollos_jumbo,
            COUNT(*) FILTER (WHERE tipo = 'restante') AS rollos_restante,
            SUM(metros) AS metros_total,
            SUM(metros) FILTER (WHERE vendible = true) AS metros_vendibles,
            COUNT(*) FILTER (WHERE vendible = true) AS rollos_vendibles,
            COUNT(*) FILTER (WHERE vendible = false) AS rollos_no_vendibles
        FROM rollos
        WHERE warehouse = %s
        GROUP BY product_name
        ORDER BY product_name
    ''', (warehouse,))
    resumen = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        'rollos': [dict(r) for r in rollos],
        'resumen': [dict(r) for r in resumen],
        'min_metros_vendible': MIN_METROS_VENDIBLE
    })

@app.route('/api/rollos', methods=['POST'])
def add_rollo():
    """Agrega un rollo individual al inventario."""
    body = request.json
    warehouse = body.get('warehouse', 'culiacan')
    product_name = body.get('product_name', '').strip()
    metros = float(body.get('metros', 0))
    tipo = body.get('tipo', 'jumbo')  # 'jumbo' o 'restante'
    notas = body.get('notas', '')

    if not product_name or metros <= 0:
        return jsonify({'error': 'product_name y metros son requeridos'}), 400
    if warehouse not in WAREHOUSES:
        return jsonify({'error': 'Almacén inválido'}), 400
    if tipo not in ('jumbo', 'restante'):
        return jsonify({'error': 'tipo debe ser jumbo o restante'}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO rollos (warehouse, product_name, metros, tipo, notas)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, vendible
    ''', (warehouse, product_name, metros, tipo, notas))
    result = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        'ok': True,
        'id': result['id'],
        'vendible': result['vendible'],
        'metros': metros,
        'tipo': tipo
    })

@app.route('/api/rollos/<int:rollo_id>', methods=['PUT'])
def update_rollo(rollo_id):
    """Actualiza los metros de un rollo (ej: después de cortar)."""
    body = request.json
    metros = float(body.get('metros', 0))
    notas = body.get('notas', None)

    conn = get_conn()
    cur = conn.cursor()

    if notas is not None:
        cur.execute('UPDATE rollos SET metros = %s, notas = %s WHERE id = %s', (metros, notas, rollo_id))
    else:
        cur.execute('UPDATE rollos SET metros = %s WHERE id = %s', (metros, rollo_id))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({'ok': True})

@app.route('/api/rollos/<int:rollo_id>', methods=['DELETE'])
def delete_rollo(rollo_id):
    """Elimina un rollo (cuando se vende o se descarta)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute('DELETE FROM rollos WHERE id = %s', (rollo_id,))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({'ok': True})

@app.route('/api/rollos/bulk', methods=['POST'])
def bulk_add_rollos():
    """Carga múltiples rollos de una vez (para migración inicial desde Excel)."""
    body = request.json
    warehouse = body.get('warehouse', 'culiacan')
    rollos = body.get('rollos', [])  # [{product_name, metros, tipo, notas?}]

    if warehouse not in WAREHOUSES:
        return jsonify({'error': 'Almacén inválido'}), 400

    conn = get_conn()
    cur = conn.cursor()
    inserted = 0

    for r in rollos:
        product_name = str(r.get('product_name', '')).strip()
        metros = float(r.get('metros', 0))
        tipo = r.get('tipo', 'jumbo')
        notas = r.get('notas', '')

        if not product_name or metros <= 0:
            continue
        if tipo not in ('jumbo', 'restante'):
            tipo = 'jumbo'

        cur.execute('''
            INSERT INTO rollos (warehouse, product_name, metros, tipo, notas)
            VALUES (%s, %s, %s, %s, %s)
        ''', (warehouse, product_name, metros, tipo, notas))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({'ok': True, 'inserted': inserted})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
