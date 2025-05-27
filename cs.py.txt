from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime, timedelta
import os
import shutil
import json
import csv

app = Flask(__name__)

DB_FILE = "punch_log.db"
SESSION_FILE = "active_sessions.json"

LIMITS = {
    "取外卖": {"max_per_day": 2, "max_duration": timedelta(minutes=1)},
    "抽烟": {"max_per_day": 8, "max_duration": timedelta(minutes=5)},
    "厕所": {"max_per_day": 2, "max_duration": timedelta(minutes=15)},
}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id TEXT,
            type TEXT,
            start_time TEXT,
            end_time TEXT,
            overtime TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS employees (
            emp_id TEXT PRIMARY KEY
        )
    ''')
    conn.commit()
    conn.close()

def count_today(emp_id, type_):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    today_str = datetime.now().strftime("%Y-%m-%d")
    c.execute('''
        SELECT COUNT(*) FROM logs
        WHERE emp_id = ? AND type = ? AND DATE(start_time) = ?
    ''', (emp_id, type_, today_str))
    count = c.fetchone()[0]
    conn.close()
    return count

def insert_log(emp_id, type_, start, end, overtime):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO logs (emp_id, type, start_time, end_time, overtime)
        VALUES (?, ?, ?, ?, ?)
    ''', (emp_id, type_, start, end, "是" if overtime else "否"))
    conn.commit()
    conn.close()

def load_active_sessions():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE, "r", encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_active_sessions(sessions):
    with open(SESSION_FILE, "w", encoding='utf-8') as f:
        json.dump(sessions, f)

def remove_active_session(emp_id, type_):
    sessions = load_active_sessions()
    key = f"{emp_id}_{type_}"
    if key in sessions:
        del sessions[key]
        save_active_sessions(sessions)

@app.route('/add_employee', methods=['POST'])
def add_employee():
    emp_id = request.json.get('emp_id', '').strip()
    if not emp_id:
        return jsonify({'success': False, 'message': '工号不能为空'})
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO employees (emp_id) VALUES (?)", (emp_id,))
        conn.commit()
        return jsonify({'success': True, 'message': f'工号 {emp_id} 添加成功'})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': '工号已存在'})
    finally:
        conn.close()

@app.route('/check_employee', methods=['POST'])
def check_employee():
    emp_id = request.json.get('emp_id', '').strip()
    if not emp_id:
        return jsonify({'exists': False})
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM employees WHERE emp_id = ?", (emp_id,))
    exists = c.fetchone() is not None
    conn.close()
    return jsonify({'exists': exists})

@app.route('/start_session', methods=['POST'])
def start_session():
    emp_id = request.json.get('emp_id', '').strip()
    type_ = request.json.get('type', '').strip()
    if count_today(emp_id, type_) >= LIMITS[type_]["max_per_day"]:
        return jsonify({'success': False, 'message': f"今天{type_}已达上限。"})
    sessions = load_active_sessions()
    key = f"{emp_id}_{type_}"
    if key in sessions:
        return jsonify({'success': False, 'message': f"该{type_}会话已经存在"})
    sessions[key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_active_sessions(sessions)
    return jsonify({'success': True, 'message': f'{type_}会话已开始'})

@app.route('/end_session', methods=['POST'])
def end_session():
    emp_id = request.json.get('emp_id', '').strip()
    type_ = request.json.get('type', '').strip()
    sessions = load_active_sessions()
    key = f"{emp_id}_{type_}"
    if key not in sessions:
        return jsonify({'success': False, 'message': f"没有正在进行的{type_}会话"})
    start_time_str = sessions[key]
    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.now()
    duration = end_time - start_time
    overtime = duration > LIMITS[type_]["max_duration"]
    insert_log(emp_id, type_, start_time_str, end_time.strftime("%Y-%m-%d %H:%M:%S"), overtime)
    remove_active_session(emp_id, type_)
    return jsonify({
        'success': True,
        'message': f"{type_}结束，用时 {duration.seconds // 60} 分钟，{'超时' if overtime else '未超时'}。",
        'duration_minutes': duration.seconds // 60,
        'overtime': overtime
    })

@app.route('/count_today', methods=['POST'])
def count_today_api():
    emp_id = request.json.get('emp_id', '').strip()
    type_ = request.json.get('type', '').strip()
    count = count_today(emp_id, type_)
    return jsonify({'count': count})

from flask import Response
import io
import csv
from datetime import datetime, timedelta
import sqlite3
from flask import request, jsonify

@app.route('/export_logs', methods=['POST'])
def export_logs():
    period = request.json.get('period', 'this_month')
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    today = datetime.now()

    if period == 'this_month':
        first_day_this_month = today.replace(day=1)
        first_day_next_month = (first_day_this_month + timedelta(days=32)).replace(day=1)
        c.execute('''
            SELECT emp_id, type, start_time, end_time, overtime
            FROM logs
            WHERE DATE(start_time) >= ? AND DATE(start_time) < ?
        ''', (first_day_this_month.strftime('%Y-%m-%d'), first_day_next_month.strftime('%Y-%m-%d')))
    elif period == 'last_month':
        first_day_this_month = today.replace(day=1)
        last_month_end = first_day_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        c.execute('''
            SELECT emp_id, type, start_time, end_time, overtime
            FROM logs
            WHERE DATE(start_time) >= ? AND DATE(start_time) <= ?
        ''', (last_month_start.strftime('%Y-%m-%d'), last_month_end.strftime('%Y-%m-%d')))
    else:
        return jsonify({'success': False, 'message': '无效的时间段'})

    rows = c.fetchall()
    conn.close()

    # 在内存中写CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["工号", "类型", "开始时间", "结束时间", "是否超时"])
    writer.writerows(rows)
    csv_data = output.getvalue()
    output.close()

    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{period}_logs_{now_str}.csv"

    # 返回响应，带文件下载提示头
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": "text/csv; charset=utf-8"
        }
    )


if __name__ == "__main__":
    init_db()
    if not os.path.exists("db_backups"):
        os.makedirs("db_backups")
    shutil.copy(DB_FILE, os.path.join("db_backups", f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"))
    app.run(host="0.0.0.0", port=5000)
