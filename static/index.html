from flask import Flask, request, jsonify, send_from_directory
import json, os, openpyxl
from datetime import datetime

app = Flask(__name__, static_folder='static')

DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

WAREHOUSES = ['culiacan', 'cdmx', 'oax']

def data_file(warehouse):
    return f'{DATA_DIR}/inventory_{warehouse}.json'

def load_data(warehouse):
    f = data_file(warehouse)
    if os.path.exists(f):
        with open(f, 'r', encoding='utf-8') as fp:
            return json.load(fp)
    seed_file = 'products_seed.json'
    if os.path.exists(seed_file):
        with open(seed_file, 'r', encoding='utf-8') as fp:
            products = json.load(fp)
    else:
        products = []
    return {
        'products': {p['name']: {'cat': p['cat'], 'min_stock': 0} for p in products},
        'snapshots': {},
        'min_stocks': {}
    }

def save_data(warehouse, data):
    with open(data_file(warehouse), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
    
    data = load_data(warehouse)
    
    for name, info in parsed.items():
        if name not in data['products']:
            data['products'][name] = {'cat': info['cat'], 'min_stock': 0}
    
    snapshot = {name: info['total'] for name, info in parsed.items()}
    data['snapshots'][report_date] = snapshot
    
    save_data(warehouse, data)
    return jsonify({'ok': True, 'date': report_date, 'count': len(parsed)})

@app.route('/api/dashboard')
def dashboard():
    warehouse = request.args.get('warehouse', 'culiacan')
    if warehouse not in WAREHOUSES:
        return jsonify({'error': 'Invalid warehouse'}), 400

    data = load_data(warehouse)
    snapshots = data['snapshots']
    products_meta = data['products']
    
    if not snapshots:
        return jsonify({'products': [], 'has_data': False})
    
    def parse_date(d):
        try:
            parts = d.split('/')
            return (int(parts[2]), int(parts[1]), int(parts[0]))
        except:
            return (0,0,0)
    
    sorted_dates = sorted(snapshots.keys(), key=parse_date)
    latest_date = sorted_dates[-1]
    latest = snapshots[latest_date]
    
    avg_consumption = {}
    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i-1]
        curr_date = sorted_dates[i]
        prev = snapshots[prev_date]
        curr = snapshots[curr_date]
        for name in curr:
            if name in prev and prev[name] > curr[name]:
                consumed = prev[name] - curr[name]
                if name not in avg_consumption:
                    avg_consumption[name] = []
                avg_consumption[name].append(consumed)
    
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
    data = load_data(warehouse)
    name = body.get('name')
    value = body.get('value', 0)
    if name in data['products']:
        data['products'][name]['min_stock'] = value
        save_data(warehouse, data)
    return jsonify({'ok': True})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
