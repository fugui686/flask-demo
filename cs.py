import os
import sys
import csv
import time
import random
import pytz
import requests
import configparser
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from PyQt5.QtCore import QByteArray
from PyQt5.QtGui import QPixmap, QIcon
import pandas as pd
from openpyxl import load_workbook
from PyQt5.QtWidgets import QMessageBox
from PyQt5 import QtWidgets, QtCore


# ========================= 通用常量 =========================
BJ_TZ = pytz.timezone('Asia/Shanghai')

FIRST_FILE_DEFAULT = "first_deposit.csv"
RECHARGE_FILE_DEFAULT = "daily_recharge.csv"

# 内部留存计算天数（1~60，内部用）
留存天数列表 = [i for i in range(1, 61)]

# 导出展示留存天数
展示留存天数 = [2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30]
渠道展示留存天数 = [2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30]

RETENTION_WINDOW_DAYS = 60

# ========================= 日志（线程安全） =========================
class LogEmitter(QtCore.QObject):
    message = QtCore.pyqtSignal(str)

log_emitter = LogEmitter()

def log(msg: str):
    print(msg)
    log_emitter.message.emit(msg)


# ========================= 配置对象 =========================
@dataclass
class AppConfig:
    平台ID: str
    子平台ID: str
    ht: str      # 不带 http(s):// 的域名，结尾不含 '/'
    token: str   # Cookie

    @staticmethod
    def from_ui_fields(平台ID: str, 子平台ID: str, ht: str, token: str) -> "AppConfig":
        ht_norm = ht.strip()
        ht_norm = ht_norm.removeprefix("https://").removeprefix("http://").rstrip("/")
        return AppConfig(
            平台ID=平台ID.strip(),
            子平台ID=子平台ID.strip(),
            ht=ht_norm,
            token=token.strip(),
        )


# ========================= 路径 & 配置文件 =========================
def is_frozen():
    return getattr(sys, 'frozen', False)

def app_dir():
    if is_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def user_config_dir():
    if sys.platform.startswith("win"):
        base = os.environ.get("APPDATA", os.path.expanduser("~"))
        return os.path.join(base, "RetentionTool")
    else:
        base = os.path.join(os.path.expanduser("~"), ".config")
        return os.path.join(base, "retention_tool")

def is_dir_writable(d: str) -> bool:
    try:
        os.makedirs(d, exist_ok=True)
        testfile = os.path.join(d, ".write_test.tmp")
        with open(testfile, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(testfile)
        return True
    except Exception:
        return False

def resolve_config_path_for_load() -> str:
    p1 = os.path.join(app_dir(), "config.ini")
    if os.path.exists(p1):
        return p1
    p2dir = user_config_dir()
    p2 = os.path.join(p2dir, "config.ini")
    return p2

def resolve_config_path_for_save() -> str:
    d1 = app_dir()
    if is_dir_writable(d1):
        return os.path.join(d1, "config.ini")
    d2 = user_config_dir()
    os.makedirs(d2, exist_ok=True)
    return os.path.join(d2, "config.ini")

def load_config() -> AppConfig:
    cfgp = resolve_config_path_for_load()
    cp = configparser.ConfigParser()
    if not os.path.exists(cfgp):
        return AppConfig(平台ID="", 子平台ID="0", ht="", token="")
    cp.read(cfgp, encoding="utf-8")
    sec = cp["DEFAULT"]
    return AppConfig(
        平台ID=sec.get("platform_id", ""),
        子平台ID=sec.get("child_id", "0"),
        ht=sec.get("ht", ""),
        token=sec.get("token", ""),
    )

def save_config(cfg: AppConfig):
    cfgp = resolve_config_path_for_save()
    cp = configparser.ConfigParser()
    cp["DEFAULT"] = {
        "platform_id": cfg.平台ID,
        "child_id": cfg.子平台ID,
        "ht": cfg.ht,
        "token": cfg.token,
    }
    with open(cfgp, "w", encoding="utf-8") as f:
        cp.write(f)
    log(f"配置已保存: {cfgp}")


# ========================= 子平台别名映射 =========================
CHILD_ALIAS_FILE = "child_alias.txt"

def get_child_alias_file_path() -> str:
    return os.path.join(app_dir(), CHILD_ALIAS_FILE)

def load_child_alias_map() -> Dict[str, str]:
    default_map: Dict[str, str] = {"2610": "B01", "2706": "B02"}
    path = get_child_alias_file_path()
    alias_map: Dict[str, str] = {}

    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if k and v:
                        alias_map[k] = v
        except Exception as e:
            log(f"[子平台别名] 读取 {path} 失败：{e}，将使用默认映射。")
            alias_map = {}

    if not os.path.exists(path):
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("# 子平台别名映射文件\n")
                f.write("# 格式：child_id=alias，例如：2610=B01\n")
                for k, v in default_map.items():
                    f.write(f"{k}={v}\n")
            log(f"[子平台别名] 未找到 {path}，已自动创建默认文件。")
        except Exception as e:
            log(f"[子平台别名] 自动创建 {path} 失败：{e}")
        alias_map = default_map.copy()

    for k, v in default_map.items():
        alias_map.setdefault(k, v)

    return alias_map

CHILD_ALIAS_MAP: Dict[str, str] = load_child_alias_map()


# ========================= ✅ 新增：数据目录（按站点/按月存放） =========================
def data_root_dir() -> Path:
    p = Path(app_dir()) / "data"
    p.mkdir(parents=True, exist_ok=True)
    return p

def month_dir(child_id: str, d: date) -> Path:
    p = data_root_dir() / str(child_id) / d.strftime("%Y-%m")
    p.mkdir(parents=True, exist_ok=True)
    return p

def month_csv_path(child_id: str, d: date, kind: str) -> Path:
    md = month_dir(child_id, d)
    if kind == "first":
        return md / FIRST_FILE_DEFAULT
    if kind == "recharge":
        return md / RECHARGE_FILE_DEFAULT
    raise ValueError("kind must be 'first' or 'recharge'")

def iter_month_keys(start_d: date, end_d: date) -> List[str]:
    if start_d > end_d:
        start_d, end_d = end_d, start_d
    cur = date(start_d.year, start_d.month, 1)
    endm = date(end_d.year, end_d.month, 1)
    out = []
    while cur <= endm:
        out.append(cur.strftime("%Y-%m"))
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out

def month_file_path(child_id: str, month_key: str, filename: str) -> Path:
    return data_root_dir() / str(child_id) / month_key / filename

def read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path: Path, fieldnames: List[str], rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def append_csv(path: Path, fieldnames: List[str], rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def read_rows_by_month_range(child_id: str, start_d: date, end_d: date,
                            filename: str, date_field: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for mk in iter_month_keys(start_d, end_d):
        p = month_file_path(child_id, mk, filename)
        if p.exists():
            rows.extend(read_csv(p))

    out: List[Dict[str, Any]] = []
    for r in rows:
        ds = (r.get(date_field) or "").strip()
        if not ds:
            continue
        try:
            dd = datetime.fromisoformat(ds).date()
        except Exception:
            continue
        if start_d <= dd <= end_d:
            out.append(r)
    return out

def read_all_rows(child_id: str, filename: str) -> List[Dict[str, Any]]:
    base = data_root_dir() / str(child_id)
    if not base.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for p in sorted(base.iterdir()):
        if p.is_dir():
            fp = p / filename
            if fp.exists():
                rows.extend(read_csv(fp))
    return rows


# ========================= 工具函数（时间范围） =========================
def day_ts_range(d: date) -> Tuple[int, int]:
    start = BJ_TZ.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
    end = BJ_TZ.localize(datetime(d.year, d.month, d.day, 23, 59, 59))
    return int(start.timestamp()), int(end.timestamp())


# ========================= ✅ 保持你原版的 headers（别动） =========================
def get_headers(cfg: AppConfig) -> Dict[str, str]:
    if not cfg.ht or not cfg.token or not cfg.平台ID:
        raise RuntimeError("配置缺失：请先填写 ht、token、平台ID")
    return {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'zh-CN,zh;q=0.9',
        'childsitecode': cfg.子平台ID or '0',
        'companycode': cfg.平台ID,
        'content-type': 'application/json',
        "cookie": cfg.token.strip(),
        'loginbacktype': '3',
        'sitecode': cfg.平台ID,
    }


def post_json_with_retry(url: str, headers: Dict[str, str], payload: Dict[str, Any],
                         retries: int = 2, timeout: int = 20) -> Optional[Dict[str, Any]]:
    for attempt in range(1, retries + 2):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if resp.status_code != 200:
                log(f"[HTTP {resp.status_code}] {url}")
                log(resp.text[:300])
                if 500 <= resp.status_code < 600 and attempt <= retries:
                    time.sleep(1.0 * attempt)
                    continue
                return None
            try:
                js = resp.json()
                if not isinstance(js, dict):
                    log("[错误] 返回非 JSON 对象")
                    return None
                return js
            except Exception as e:
                log(f"[错误] JSON解析失败: {e}")
                log(resp.text[:300])
                return None
        except requests.RequestException as e:
            log(f"[异常] 请求失败({attempt}/{retries + 1})：{e}")
            if attempt <= retries:
                time.sleep(1.0 * attempt)
                continue
            return None
    return None


# ========================= 渠道配置读取（B 版：按前缀扫首存表） =========================
def load_channel_groups_for_child(child_id: str) -> Dict[str, set]:
    """
    B 版本逻辑（保持原思路），但首存数据从 data/<child_id>/ 各月份读取。
    - 文件名：{child_id}_xxx.txt -> xxx 作为渠道前缀
    - 从首存表 channel.startswith(前缀) 找到匹配渠道
    """
    base_dir = app_dir()
    groups: Dict[str, set] = {}
    if not os.path.isdir(base_dir):
        return groups

    first_rows = read_all_rows(child_id, FIRST_FILE_DEFAULT)
    all_channels = {(r.get("channel") or "").strip() for r in first_rows if r.get("channel")}
    if not all_channels:
        log(f"[渠道配置][{child_id}] data/{child_id}/ 下首存表没有渠道字段，按前缀统计将跳过。")

    for fname in os.listdir(base_dir):
        if not fname.endswith(".txt"):
            continue
        if not fname.startswith(f"{child_id}_"):
            continue

        group_name = fname[len(child_id) + 1:-4].strip()
        if not group_name:
            continue

        matched = {ch for ch in all_channels if ch.startswith(group_name)}
        if matched:
            groups[group_name] = matched
            log(f"[渠道配置][{child_id}] 渠道组 {group_name}（前缀）匹配到 {len(matched)} 个渠道。")
        else:
            log(f"[渠道配置][{child_id}] 渠道组 {group_name} 未匹配到任何渠道。")

    if not groups:
        log(f"[渠道配置][{child_id}] 未找到任何有效的渠道组前缀（形如 {child_id}_xxx.txt），将跳过按渠道留存。")
    return groups


# ========================= 核心业务：抓首存 =========================
def fetch_first_deposit_for_day(cfg: AppConfig, d: date) -> List[Dict[str, Any]]:
    start_ts, end_ts = day_ts_range(d)
    url = f"https://{cfg.ht}/api/go-gateway-internal/user/advancedGetUserListV2"

    all_rows: List[Dict[str, Any]] = []
    page = 1
    size = 1000
    d_str = d.strftime("%Y-%m-%d")
    headers = get_headers(cfg)

    while True:
        payload = {
            'selectTimeKey': 2,
            'accountTypes': [],
            'currency': 'CNY',
            'current': page,
            'size': size,
            'firstPayTimeFrom': start_ts,
            'firstPayTimeTo': end_ts,
        }

        js = post_json_with_retry(url, headers, payload)
        if not js:
            log(f"[首存][{cfg.子平台ID}] {d_str} 第{page}页 获取失败")
            break

        data_wrapper = js.get('data') or {}
        data = data_wrapper.get('data') or data_wrapper.get('list') or []
        if not data:
            break

        for item in data:
            user_id = item.get('useridx') or item.get('userIdx')
            if not user_id:
                continue

            first_amount_raw = item.get('firstPayAmount')
            try:
                first_amount = float(first_amount_raw) if first_amount_raw is not None else 0.0
            except Exception:
                first_amount = 0.0

            if first_amount <= 0:
                continue

            channel = item.get('regpkgidName') or ""
            all_rows.append({
                "user_id": str(user_id),
                "first_date": d_str,
                "first_amount": first_amount,
                "channel": str(channel).strip(),
            })

        if len(data) < size:
            break
        page += 1
        time.sleep(random.uniform(1, 3))

    log(f"[首存][{cfg.子平台ID}] {d_str} 获取到 {len(all_rows)} 条记录")
    return all_rows


def save_first_deposit(cfg: AppConfig, d: date, new_rows: List[Dict[str, Any]]):
    """✅ 按月份落地：data/<child>/<YYYY-MM>/first_deposit.csv"""
    if not new_rows:
        return
    path = month_csv_path(cfg.子平台ID, d, "first")
    fieldnames = ["user_id", "first_date", "first_amount", "channel"]
    exist = read_csv(path)
    by_user = {r["user_id"]: r for r in exist if r.get("user_id")}

    for r in new_rows:
        uid = r["user_id"]
        if uid in by_user:
            if r["first_date"] < by_user[uid]["first_date"]:
                by_user[uid] = r
        else:
            by_user[uid] = r

    write_csv(path, fieldnames, by_user.values())


# ========================= 核心业务：抓会员报表充值 =========================
def fetch_member_report_for_day(cfg: AppConfig, d: date) -> List[Dict[str, Any]]:
    start_ts, end_ts = day_ts_range(d)
    url = f"https://{cfg.ht}/api/go-gateway-internal/noEncrypt/statistics/report/user_report"

    all_rows: List[Dict[str, Any]] = []
    page = 1
    size = 1000
    d_str = d.strftime("%Y-%m-%d")
    headers = get_headers(cfg)

    while True:
        # ✅ 保持你原版的分页字段 pageSort
        payload = {
            'currency': 'CNY',
            'startTime': start_ts,
            'endTime': end_ts,
            'childSiteCode': cfg.子平台ID or cfg.平台ID,
            'pageSort': {'page': page, 'limit': size},
        }

        js = post_json_with_retry(url, headers, payload)
        if not js:
            log(f"[会员报表][{cfg.子平台ID}] {d_str} 第{page}页 获取失败")
            break

        data_wrapper = js.get('data') or {}
        data = data_wrapper.get('list') or data_wrapper.get('data') or []
        if not data:
            break

        for item in data:
            user_id = item.get('userIdx') or item.get('useridx')
            if not user_id:
                continue

            deposit_raw = item.get('deposit', '0')
            try:
                deposit = float(deposit_raw)
            except (ValueError, TypeError):
                deposit = 0.0

            if deposit > 0:
                all_rows.append({
                    "user_id": str(user_id),
                    "pay_date": d_str,
                    "pay_amount": deposit,
                })

        if len(data) < size:
            break
        page += 1
        time.sleep(random.uniform(1, 3))

    log(f"[会员报表][{cfg.子平台ID}] {d_str} 获取到 {len(all_rows)} 条记录")
    return all_rows


def save_member_report(cfg: AppConfig, d: date, new_rows: List[Dict[str, Any]]):
    """✅ 按月份落地：data/<child>/<YYYY-MM>/daily_recharge.csv"""
    if not new_rows:
        return
    path = month_csv_path(cfg.子平台ID, d, "recharge")
    fieldnames = ["user_id", "pay_date", "pay_amount"]

    exist = read_csv(path)
    seen = {(r.get("user_id"), r.get("pay_date"), str(r.get("pay_amount"))) for r in exist}
    to_write = []
    for r in new_rows:
        key = (r.get("user_id"), r.get("pay_date"), str(r.get("pay_amount")))
        if key in seen:
            continue
        seen.add(key)
        to_write.append(r)

    if to_write:
        append_csv(path, fieldnames, to_write)


# ========================= 留存计算（读取跨月聚合） =========================
def calc_retention_compact(cfg: AppConfig, last_pay_date: str, output_path: Optional[str] = None) -> str:
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    def _tier_label(first_amt: float) -> Optional[int]:
        try:
            x = float(first_amt)
        except Exception:
            return None
        if x <= 0:
            return None
        if x <= 30:
            return 30
        if 31 <= x <= 50:
            return 50
        if 51 <= x <= 100:
            return 51
        if x >= 101:
            return 101
        return None

    tier_display_map: Dict[int, str] = {30: "30", 50: "50", 51: "51-100", 101: "101+"}
    tier_text_to_key: Dict[str, int] = {v: k for k, v in tier_display_map.items()}

    def _apply_base_style(ws, max_row: int, max_col: int):
        base_fill = PatternFill(fill_type="solid", fgColor="D9D9D9")
        font_base = Font(name="宋体", size=9)
        font_bold = Font(name="宋体", size=9, bold=True)
        align_center = Alignment(horizontal="center", vertical="center")
        for r in range(1, max_row + 1):
            for c in range(1, max_col + 1):
                cell = ws.cell(row=r, column=c)
                cell.fill = base_fill
                cell.alignment = align_center
                cell.font = font_bold if r <= 2 else font_base

    def _apply_day_color_blocks(ws, max_row: int, display_days: List[int], start_col_base: int):
        COLOR_RED = "FF3B5B"
        COLOR_GREEN = "4DAC24"
        COLOR_ORANGE = "B47700"
        FILL_RED = "FFC7CE"
        FILL_GREEN = "DCF8DA"
        FILL_ORANGE = "FFE699"

        day_font_colors: Dict[int, str] = {2: COLOR_GREEN, 3: COLOR_ORANGE, 4: COLOR_RED}
        day_fill_colors: Dict[int, str] = {2: FILL_GREEN, 3: FILL_ORANGE, 4: FILL_RED}

        rest_days = [d for d in display_days if d not in (2, 3, 4)]
        cycle_fc = [COLOR_GREEN, COLOR_ORANGE, COLOR_RED]
        cycle_fl = [FILL_GREEN, FILL_ORANGE, FILL_RED]
        for idx, day in enumerate(rest_days):
            day_font_colors[day] = cycle_fc[idx % 3]
            day_fill_colors[day] = cycle_fl[idx % 3]

        for idx, day in enumerate(display_days):
            start_col = start_col_base + idx * 3
            end_col = start_col + 2
            fc = day_font_colors.get(day, COLOR_GREEN)
            fill = PatternFill(fill_type="solid", fgColor=day_fill_colors.get(day, FILL_GREEN))
            for col_idx in range(start_col, end_col + 1):
                for r in range(1, max_row + 1):
                    cell = ws.cell(row=r, column=col_idx)
                    cell.font = cell.font.copy(color=fc)
                    if cell.value not in (None, ""):
                        cell.fill = fill

    def _apply_tier_row_colors(ws2, max_row: int, max_col: int):
        tier_fill_map: Dict[int, PatternFill] = {
            30: PatternFill(fill_type="solid", fgColor="E2F0D9"),
            50: PatternFill(fill_type="solid", fgColor="FFF2CC"),
            51: PatternFill(fill_type="solid", fgColor="D9E1F2"),
            101: PatternFill(fill_type="solid", fgColor="E4DFEC"),
        }
        for r in range(3, max_row + 1):
            raw = ws2.cell(row=r, column=2).value
            if raw in (None, ""):
                continue
            tier_key: Optional[int] = None
            if isinstance(raw, (int, float)):
                try:
                    tier_key = int(raw)
                except Exception:
                    tier_key = None
            else:
                s = str(raw).strip()
                tier_key = tier_text_to_key.get(s)
            if not tier_key:
                continue
            fill = tier_fill_map.get(tier_key)
            if not fill:
                continue
            for c in range(2, max_col + 1):
                ws2.cell(row=r, column=c).fill = fill

    last_pay = datetime.fromisoformat(last_pay_date).date()
    cutoff_start = last_pay - timedelta(days=RETENTION_WINDOW_DAYS - 1)

    if output_path is None:
        suffix = datetime.now().strftime("%m月%d日%H时%M分%S秒")
        alias = CHILD_ALIAS_MAP.get(cfg.子平台ID, cfg.子平台ID)
        output_path = f"{alias}-30日留存表_{suffix}.xlsx"

    # ✅ 跨月读取（只读窗口范围）
    first_rows = read_rows_by_month_range(cfg.子平台ID, cutoff_start, last_pay, FIRST_FILE_DEFAULT, "first_date")
    recharge_rows = read_rows_by_month_range(cfg.子平台ID, cutoff_start, last_pay, RECHARGE_FILE_DEFAULT, "pay_date")
    if not first_rows or not recharge_rows:
        raise RuntimeError("首存表或充值表为空，无法计算留存（请先跑补历史/每日更新生成 data 目录数据）")

    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for r in first_rows:
        d = r.get("first_date")
        if not d:
            continue
        by_date.setdefault(d, []).append(r)
    if not by_date:
        raise RuntimeError("没有首存数据")

    recharge_uid_index: Dict[str, set] = {}
    recharge_amt_index: Dict[str, Dict[str, float]] = {}
    for r in recharge_rows:
        d = r.get("pay_date")
        uid = r.get("user_id")
        if not d or not uid:
            continue
        try:
            amt = float(r.get("pay_amount", 0) or 0)
        except (TypeError, ValueError):
            amt = 0.0
        recharge_uid_index.setdefault(d, set()).add(uid)
        m = recharge_amt_index.setdefault(d, {})
        m[uid] = m.get(uid, 0.0) + amt

    # ========================= 1) 总表（不动） =========================
    rows: List[Dict[str, Any]] = []
    for d in sorted(by_date.keys()):
        d0 = datetime.fromisoformat(d).date()
        if d0 < cutoff_start or d0 > last_pay:
            continue

        base_users = by_date[d]
        base_ids = {u.get("user_id") for u in base_users if u.get("user_id")}
        base_count = len(base_ids)

        base_amount = 0.0
        for u in base_users:
            try:
                base_amount += float(u.get("first_amount", 0) or 0)
            except Exception:
                pass

        row: Dict[str, Any] = {
            "首存日期": d,
            "首存人数": base_count,
            "首存金额": int(round(base_amount)) if base_amount else 0
        }

        if base_count > 0:
            max_day_label = (last_pay - d0).days + 1
            tmp: Dict[int, Dict[str, Any]] = {}
            for N in 留存天数列表:
                target = d0 + timedelta(days=N)
                if target > last_pay:
                    break
                t_str = target.strftime("%Y-%m-%d")
                uid_set = recharge_uid_index.get(t_str, set())
                amt_map = recharge_amt_index.get(t_str, {})
                if not uid_set:
                    tmp[N + 1] = {"人数": 0, "金额": 0.0, "率": 0.0}
                    continue
                inter = base_ids & uid_set
                keep_num = len(inter)
                keep_amt = sum(amt_map.get(uid, 0.0) for uid in inter)
                rate = keep_num / base_count if keep_num > 0 else 0.0
                tmp[N + 1] = {"人数": keep_num, "金额": keep_amt, "率": rate}

            for day in 展示留存天数:
                col_rate = f"{day}日留存率"
                col_num = f"{day}日人数"
                col_amt = f"{day}日金额"
                if day > max_day_label:
                    row[col_rate] = ""
                    row[col_num] = ""
                    row[col_amt] = ""
                else:
                    data = tmp.get(day)
                    if data:
                        row[col_rate] = round(float(data["率"]), 4)
                        row[col_num] = int(data["人数"])
                        row[col_amt] = int(round(float(data["金额"]))) if data["金额"] else 0
                    else:
                        row[col_rate] = 0.0
                        row[col_num] = 0
                        row[col_amt] = 0

        rows.append(row)

    df = pd.DataFrame(rows)
    columns = ["首存日期", "首存人数", "首存金额"]
    for day in 展示留存天数:
        columns += [f"{day}日留存率", f"{day}日人数", f"{day}日金额"]

    if not df.empty:
        df = df[columns]
    else:
        df = pd.DataFrame(columns=columns)

    df.to_excel(output_path, index=False, header=False, startrow=2, engine="openpyxl")

    wb = load_workbook(output_path)
    ws = wb.active
    alias = CHILD_ALIAS_MAP.get(cfg.子平台ID, cfg.子平台ID)

    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=3)
    ws.cell(row=1, column=1, value=f"{alias}-2-30日充值留存表")

    col = 4
    for day in 展示留存天数:
        ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col + 2)
        ws.cell(row=1, column=col, value=f"{day}日")
        col += 3

    ws.cell(row=2, column=1, value="统计日期")
    ws.cell(row=2, column=2, value="首存人数")
    ws.cell(row=2, column=3, value="首存金额")
    col = 4
    for _ in 展示留存天数:
        ws.cell(row=2, column=col, value="留存率")
        ws.cell(row=2, column=col + 1, value="人数")
        ws.cell(row=2, column=col + 2, value="金额")
        col += 3

    first_data_row = 3
    data_rows = len(df)
    last_data_row = first_data_row + data_rows - 1 if data_rows > 0 else first_data_row - 1
    max_col = ws.max_column
    max_row = max(2, last_data_row)

    for col_idx in range(1, max_col + 1):
        col_letter = ws.cell(row=2, column=col_idx).column_letter
        ws.column_dimensions[col_letter].width = 8 if col_idx <= 3 else 6

    _apply_base_style(ws, max_row, max_col)

    COLOR_RED = "FF3B5B"
    FILL_RED = "FFC7CE"
    red_fill = PatternFill(fill_type="solid", fgColor=FILL_RED)
    for col_idx in range(1, 4):
        for r in range(1, max_row + 1):
            cell = ws.cell(row=r, column=col_idx)
            cell.font = cell.font.copy(color=COLOR_RED)
            if cell.value not in (None, ""):
                cell.fill = red_fill

    _apply_day_color_blocks(ws, max_row, 展示留存天数, start_col_base=4)

    col_index = 4
    for _ in 展示留存天数:
        rate_col_letter = ws.cell(row=2, column=col_index).column_letter
        if data_rows > 0:
            for r in range(first_data_row, last_data_row + 1):
                cell = ws[f"{rate_col_letter}{r}"]
                if cell.value not in (None, ""):
                    cell.number_format = "0.00%"
        col_index += 3

    if data_rows > 0:
        for r in range(first_data_row, last_data_row + 1):
            c = ws["C" + str(r)]
            if c.value not in (None, ""):
                c.number_format = "0"

        amt_col_index = 6
        for _ in 展示留存天数:
            amt_col_letter = ws.cell(row=2, column=amt_col_index).column_letter
            for r in range(first_data_row, last_data_row + 1):
                cell = ws[f"{amt_col_letter}{r}"]
                if cell.value not in (None, ""):
                    cell.number_format = "0"
            amt_col_index += 3

    # ========================= 2) 金额档位留存 =========================
    sheet_name = "金额档位留存"
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws2 = wb.create_sheet(sheet_name)

    ws2.merge_cells(start_row=1, start_column=1, end_row=1, end_column=4)
    ws2.cell(row=1, column=1, value=f"{alias}-金额档位留存表")

    col = 5
    for day in 展示留存天数:
        ws2.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col + 2)
        ws2.cell(row=1, column=col, value=f"{day}日")
        col += 3

    ws2.cell(row=2, column=1, value="统计日期")
    ws2.cell(row=2, column=2, value="金额档位")
    ws2.cell(row=2, column=3, value="首存人数")
    ws2.cell(row=2, column=4, value="首存金额")

    col = 5
    for _ in 展示留存天数:
        ws2.cell(row=2, column=col, value="留存率")
        ws2.cell(row=2, column=col + 1, value="人数")
        ws2.cell(row=2, column=col + 2, value="金额")
        col += 3

    tier_order = [30, 50, 51, 101]
    row_ptr = 3

    for d in sorted(by_date.keys()):
        d0 = datetime.fromisoformat(d).date()
        if d0 < cutoff_start or d0 > last_pay:
            continue

        base_users = by_date[d]
        tier_users: Dict[int, List[Dict[str, Any]]] = {k: [] for k in tier_order}
        for u in base_users:
            try:
                fa = float(u.get("first_amount", 0) or 0)
            except Exception:
                fa = 0.0
            label = _tier_label(fa)
            if label in tier_users:
                tier_users[label].append(u)

        block_start_row = row_ptr
        max_day_label = (last_pay - d0).days + 1

        for tier_val in tier_order:
            users = tier_users.get(tier_val, [])
            ids = {u.get("user_id") for u in users if u.get("user_id")}
            cnt = len(ids)

            first_amt_sum = 0.0
            for u in users:
                try:
                    first_amt_sum += float(u.get("first_amount", 0) or 0)
                except Exception:
                    pass

            ws2.cell(row=row_ptr, column=1, value=d)
            ws2.cell(row=row_ptr, column=2, value=tier_display_map.get(tier_val, str(tier_val)))
            ws2.cell(row=row_ptr, column=3, value=cnt)
            ws2.cell(row=row_ptr, column=4, value=int(round(first_amt_sum)) if first_amt_sum else 0)

            col_ptr = 5
            for day in 展示留存天数:
                if day > max_day_label:
                    ws2.cell(row=row_ptr, column=col_ptr, value="")
                    ws2.cell(row=row_ptr, column=col_ptr + 1, value="")
                    ws2.cell(row=row_ptr, column=col_ptr + 2, value="")
                elif cnt <= 0:
                    ws2.cell(row=row_ptr, column=col_ptr, value=0)
                    ws2.cell(row=row_ptr, column=col_ptr + 1, value=0)
                    ws2.cell(row=row_ptr, column=col_ptr + 2, value=0)
                else:
                    target = d0 + timedelta(days=day - 1)
                    t_str = target.strftime("%Y-%m-%d")
                    uid_set = recharge_uid_index.get(t_str, set())
                    amt_map = recharge_amt_index.get(t_str, {})

                    inter = ids & uid_set if uid_set else set()
                    keep_num = len(inter)
                    keep_amt = sum(amt_map.get(uid, 0.0) for uid in inter) if inter else 0.0
                    rate = keep_num / cnt if keep_num > 0 else 0.0

                    ws2.cell(row=row_ptr, column=col_ptr, value=round(float(rate), 4))
                    ws2.cell(row=row_ptr, column=col_ptr + 1, value=int(keep_num))
                    ws2.cell(row=row_ptr, column=col_ptr + 2, value=int(round(float(keep_amt))) if keep_amt else 0)

                col_ptr += 3

            row_ptr += 1

        ws2.merge_cells(
            start_row=block_start_row,
            start_column=1,
            end_row=block_start_row + len(tier_order) - 1,
            end_column=1
        )

    ws2_max_row = max(2, row_ptr - 1)
    ws2_max_col = 4 + len(展示留存天数) * 3

    for col_idx in range(1, ws2_max_col + 1):
        col_letter = ws2.cell(row=2, column=col_idx).column_letter
        ws2.column_dimensions[col_letter].width = 8 if col_idx <= 4 else 6

    _apply_base_style(ws2, ws2_max_row, ws2_max_col)
    _apply_tier_row_colors(ws2, ws2_max_row, ws2_max_col)

    # ✅ 金额档位留存：留存率列显示百分比（从第3行开始）
    rate_col_index = 5  # 第一个“留存率”列
    for _ in 展示留存天数:
        rate_col_letter = ws2.cell(row=2, column=rate_col_index).column_letter
        for r in range(3, ws2_max_row + 1):
            cell = ws2[f"{rate_col_letter}{r}"]
            if cell.value not in (None, ""):
                cell.number_format = "0.00%"
        rate_col_index += 3

    # ✅✅✅ 只有“有内容”的单元格才画细框线（从第3行开始）
    thin = Side(style="thin", color="000000")
    thin_border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for r in range(3, ws2_max_row + 1):
        for c in range(1, ws2_max_col + 1):
            if ws2.cell(row=r, column=c).value not in (None, ""):
                ws2.cell(row=r, column=c).border = thin_border

    # ✅ B列“金额档位”加粗（从第3行开始）
    bold = Font(name="宋体", size=9, bold=True)
    for r in range(3, ws2_max_row + 1):
        cell = ws2.cell(row=r, column=2)
        if cell.value not in (None, ""):
            cell.font = bold

    wb.save(output_path)
    log(f"[导出] {output_path}")
    return output_path


# ========================= 按渠道留存（跨月读取） =========================
def calc_channel_retention_2_30(cfg: AppConfig, last_pay_date: str,
                                channel_groups: Dict[str, set]) -> Optional[str]:
    from openpyxl.styles import Alignment, Font, PatternFill

    if not channel_groups:
        return None

    # ✅ 读取“全量首存/充值”（你现在是 data/<child_id>/<YYYY-MM>/... 跨月存放）
    # 依赖你现在项目里已有的：read_all_rows / read_rows_by_month_range
    first_rows_all = read_all_rows(cfg.子平台ID, FIRST_FILE_DEFAULT)
    if not first_rows_all:
        log(f"[按渠道留存][{cfg.子平台ID}] 首存表为空（data/{cfg.子平台ID}/ 下未找到任何首存CSV），跳过。")
        return None

    last_pay = datetime.fromisoformat(last_pay_date).date()
    cutoff_start = last_pay - timedelta(days=RETENTION_WINDOW_DAYS - 1)

    # 只保留窗口范围内的首存
    first_rows: List[Dict[str, Any]] = []
    for r in first_rows_all:
        ds = (r.get("first_date") or "").strip()
        if not ds:
            continue
        try:
            dd = datetime.fromisoformat(ds).date()
        except Exception:
            continue
        if cutoff_start <= dd <= last_pay:
            first_rows.append(r)

    if not first_rows:
        log(f"[按渠道留存][{cfg.子平台ID}] 首存数据过滤后为空（窗口 {cutoff_start}~{last_pay}），跳过。")
        return None

    # 充值数据：为了性能，按“窗口范围”跨月读取
    recharge_rows = read_rows_by_month_range(cfg.子平台ID, cutoff_start, last_pay, RECHARGE_FILE_DEFAULT, "pay_date")
    if not recharge_rows:
        log(f"[按渠道留存][{cfg.子平台ID}] 充值表为空（data/{cfg.子平台ID}/ 下未找到任何充值CSV），跳过。")
        return None

    # 充值索引：日期 -> set(user_id) & 日期 -> {user_id: 当天充值总额}
    recharge_uid_index: Dict[str, set] = {}
    recharge_amt_index: Dict[str, Dict[str, float]] = {}
    for r in recharge_rows:
        d = (r.get("pay_date") or "").strip()
        uid = (r.get("user_id") or "").strip()
        if not d or not uid:
            continue
        try:
            amt = float(r.get("pay_amount", 0) or 0)
        except (TypeError, ValueError):
            amt = 0.0
        recharge_uid_index.setdefault(d, set()).add(uid)
        m = recharge_amt_index.setdefault(d, {})
        m[uid] = m.get(uid, 0.0) + amt

    # 输出文件名：用子平台别名（B01/B02）优先
    alias = CHILD_ALIAS_MAP.get(cfg.子平台ID, cfg.子平台ID)
    suffix = datetime.now().strftime("%m月%d日%H时%M分%S秒")
    output_path = f"{alias}_按渠道2-30日留存_{suffix}.xlsx"

    sheet_rows_count: Dict[str, int] = {}

    # ========================= 1) 先用 pandas 写数据（和原版一致：第4行开始写，前三行留表头） =========================
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for group_name, channels in channel_groups.items():
            safe_sheet = group_name[:31]  # Excel sheet 名限制
            # 按渠道组过滤首存，并按日期分组（只保留窗口内）
            group_by_date: Dict[str, List[Dict[str, Any]]] = {}
            for r in first_rows:
                if (r.get("channel") or "") in channels:
                    d_str = (r.get("first_date") or "").strip()
                    if not d_str:
                        continue
                    group_by_date.setdefault(d_str, []).append(r)

            rows: List[Dict[str, Any]] = []

            if group_by_date:
                # 这个渠道组自己的“最早首存日期”
                dates_obj: List[date] = []
                for ds in group_by_date.keys():
                    try:
                        dates_obj.append(datetime.fromisoformat(ds).date())
                    except Exception:
                        continue

                if dates_obj:
                    earliest_date_obj = min(dates_obj)
                    cur = earliest_date_obj
                    while cur <= last_pay:
                        d_str = cur.strftime("%Y-%m-%d")
                        base_users = group_by_date.get(d_str, [])
                        base_ids = {str(u.get("user_id")) for u in base_users if u.get("user_id")}
                        base_count = len(base_ids)

                        base_amount = 0.0
                        for u in base_users:
                            try:
                                base_amount += float(u.get("first_amount", 0) or 0)
                            except Exception:
                                pass

                        row: Dict[str, Any] = {
                            "首存日期": d_str,
                            "首存人数": base_count,
                            "首存金额": int(round(base_amount)) if base_amount else 0,
                        }

                        if base_count > 0:
                            d0 = cur
                            max_day_label = (last_pay - d0).days + 1
                            tmp: Dict[int, Dict[str, Any]] = {}

                            # 内部 1~60 日索引，再映射到展示天数
                            for N in 留存天数列表:
                                target = d0 + timedelta(days=N)
                                if target > last_pay:
                                    break
                                t_str = target.strftime("%Y-%m-%d")
                                uid_set = recharge_uid_index.get(t_str, set())

                                if not uid_set:
                                    tmp[N + 1] = {"人数": 0, "金额": 0.0, "率": 0.0}
                                    continue

                                inter = base_ids & uid_set
                                keep_num = len(inter)
                                amt_map = recharge_amt_index.get(t_str, {})
                                keep_amt = sum(amt_map.get(uid, 0.0) for uid in inter)
                                rate = (keep_num / base_count) if base_count > 0 else 0.0
                                tmp[N + 1] = {"人数": keep_num, "金额": keep_amt, "率": rate}

                            for day in 渠道展示留存天数:
                                col_num = f"{day}日留存人数"
                                col_amt = f"{day}日留存金额"
                                col_rate = f"{day}日留存率"
                                if day > max_day_label:
                                    row[col_num] = ""
                                    row[col_amt] = ""
                                    row[col_rate] = ""
                                else:
                                    data = tmp.get(day)
                                    if data:
                                        row[col_num] = int(data["人数"])
                                        row[col_amt] = int(round(data["金额"])) if data["金额"] else 0
                                        row[col_rate] = round(float(data["率"]), 4)
                                    else:
                                        row[col_num] = 0
                                        row[col_amt] = 0
                                        row[col_rate] = 0.0
                        else:
                            # 首存人数为 0：这一行每个留存列统一写 0（原逻辑）
                            for day in 渠道展示留存天数:
                                row[f"{day}日留存人数"] = 0
                                row[f"{day}日留存金额"] = 0
                                row[f"{day}日留存率"] = 0.0

                        rows.append(row)
                        cur += timedelta(days=1)

            # DataFrame 列顺序（原版）
            df = pd.DataFrame(rows)
            columns = ["首存日期", "首存人数", "首存金额"]
            for day in 渠道展示留存天数:
                columns += [f"{day}日留存人数", f"{day}日留存金额", f"{day}日留存率"]

            if df.empty:
                df = pd.DataFrame(columns=columns)
                sheet_rows_count[safe_sheet] = 0
            else:
                df = df[columns]
                sheet_rows_count[safe_sheet] = len(df)

            # ✅ 第4行开始写数据（前 1~3 行留给表头），header=False（原版）
            df.to_excel(writer, sheet_name=safe_sheet, index=False, header=False, startrow=3)

    # ========================= 2) 用 openpyxl 调整表头 & 样式（完全按你原版） =========================
    wb = load_workbook(output_path)

    align_center = Alignment(horizontal="center", vertical="center")
    font_simsun_9 = Font(name="宋体", size=9)
    font_simsun_9_bold = Font(name="宋体", size=9, bold=True)

    fill_title = PatternFill(fill_type="solid", fgColor="ACB9CA")   # 第一行背景
    fill_header = PatternFill(fill_type="solid", fgColor="FEE796")  # 第二行背景
    fill_rate = PatternFill(fill_type="solid", fgColor="C8D5E0")    # 留存率列背景

    for group_name in channel_groups.keys():
        safe_sheet = group_name[:31]
        if safe_sheet not in wb.sheetnames:
            continue

        ws = wb[safe_sheet]
        max_col = ws.max_column
        data_rows = sheet_rows_count.get(safe_sheet, 0)
        first_data_row = 4
        last_data_row = first_data_row + data_rows - 1 if data_rows > 0 else first_data_row - 1

        # 列宽：A 列 8，其它列 7（原版）
        for col_idx in range(1, max_col + 1):
            col_letter = ws.cell(row=3, column=col_idx).column_letter
            ws.column_dimensions[col_letter].width = 8 if col_letter == "A" else 7

        # 第 1、2 行高度 16（原版）
        ws.row_dimensions[1].height = 16
        ws.row_dimensions[2].height = 16

        # 计算标题需要合并到哪一列：至少到第3列；如果有数据，看最右侧有值的列（原版）
        last_used_col = 3
        if data_rows > 0 and last_data_row >= first_data_row:
            for col_idx in range(4, max_col + 1):
                has_val = False
                for r in range(first_data_row, last_data_row + 1):
                    v = ws.cell(row=r, column=col_idx).value
                    if v not in ("", None):
                        has_val = True
                        break
                if has_val:
                    last_used_col = col_idx

        # 第 1 行：别名-渠道组名留存会员（加粗）
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=last_used_col)
        cell_title = ws.cell(row=1, column=1, value=f"{alias}-{group_name}留存会员")
        cell_title.font = font_simsun_9_bold
        cell_title.alignment = align_center

        # 第 2 行：2-30日充值留存表
        ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=3)
        cell_sub = ws.cell(row=2, column=1, value="2-30日充值留存表")
        cell_sub.font = font_simsun_9_bold
        cell_sub.alignment = align_center

        col = 4
        for day in 渠道展示留存天数:
            ws.merge_cells(start_row=2, start_column=col, end_row=2, end_column=col + 2)
            c = ws.cell(row=2, column=col, value=f"{day}日")
            c.font = font_simsun_9_bold
            c.alignment = align_center
            col += 3

        # 第 3 行：列名
        headers = ["统计日期", "首存人数", "首存金额"]
        for idx, text in enumerate(headers, start=1):
            c = ws.cell(row=3, column=idx, value=text)
            c.font = font_simsun_9_bold
            c.alignment = align_center

        col = 4
        for _ in 渠道展示留存天数:
            c1 = ws.cell(row=3, column=col, value="留存人数")
            c2 = ws.cell(row=3, column=col + 1, value="留存金额")
            c3 = ws.cell(row=3, column=col + 2, value="留存率")
            for c in (c1, c2, c3):
                c.font = font_simsun_9_bold
                c.alignment = align_center
            col += 3

        # 整表字体 & 居中（从第 1 行到最后一行）
        max_row_for_font = max(3, last_data_row if last_data_row >= 1 else 3)
        for row in ws.iter_rows(min_row=1, max_row=max_row_for_font, min_col=1, max_col=max_col):
            for cell in row:
                cell.font = font_simsun_9_bold if cell.row <= 3 else font_simsun_9
                cell.alignment = align_center

        # 第一行 & 第二行背景色
        for col_idx in range(1, max_col + 1):
            ws.cell(row=1, column=col_idx).fill = fill_title
            ws.cell(row=2, column=col_idx).fill = fill_header

        # 留存率列背景色 + 百分比格式（从第 3 行开始，包括表头；数据行才设百分比）
        col_index = 4
        for _ in 渠道展示留存天数:
            rate_col_letter = ws.cell(row=3, column=col_index + 2).column_letter
            for r in range(3, max_row_for_font + 1):
                cell = ws[f"{rate_col_letter}{r}"]
                if cell.value not in ("", None):
                    cell.fill = fill_rate
                    if r >= first_data_row and data_rows > 0:
                        cell.number_format = "0.00%"
            col_index += 3

    wb.save(output_path)
    log(f"[按渠道留存][{cfg.子平台ID}] 已生成按渠道 2-30 日留存表：{output_path}")
    return output_path


# ========================= 业务入口：补历史 / 每日更新 =========================
def 补历史数据(cfg: AppConfig, 开始日期: date, 结束日期: date) -> str:
    cur = 开始日期
    while cur <= 结束日期:
        log(f"[补历史][{cfg.子平台ID}] 处理日期 {cur}")

        fd_rows = fetch_first_deposit_for_day(cfg, cur)
        if fd_rows:
            save_first_deposit(cfg, cur, fd_rows)
            log(f"[补历史-首存][{cfg.子平台ID}] {cur} 合并 {len(fd_rows)} 条")
        else:
            log(f"[补历史-首存][{cfg.子平台ID}] {cur} 无数据")

        rc_rows = fetch_member_report_for_day(cfg, cur)
        if rc_rows:
            save_member_report(cfg, cur, rc_rows)
            log(f"[补历史-报表][{cfg.子平台ID}] {cur} 合并 {len(rc_rows)} 条")
        else:
            log(f"[补历史-报表][{cfg.子平台ID}] {cur} 无数据")

        cur += timedelta(days=1)

    last_pay_date = 结束日期.strftime("%Y-%m-%d")
    path = calc_retention_compact(cfg, last_pay_date)

    if hasattr(os, "startfile"):
        try:
            os.startfile(path)
        except Exception:
            pass
    return path


def 每日更新(cfg: AppConfig) -> str:
    yesterday = date.today() - timedelta(days=1)
    log(f"[每日更新][{cfg.子平台ID}] 处理昨日 {yesterday}")

    fd_rows = fetch_first_deposit_for_day(cfg, yesterday)
    save_first_deposit(cfg, yesterday, fd_rows)

    rc_rows = fetch_member_report_for_day(cfg, yesterday)
    save_member_report(cfg, yesterday, rc_rows)

    last_pay_date = yesterday.strftime("%Y-%m-%d")
    path = calc_retention_compact(cfg, last_pay_date)

    if hasattr(os, "startfile"):
        try:
            os.startfile(path)
        except Exception:
            pass
    return path


# ========================= 子线程（支持多子平台ID循环） =========================
class Worker(QtCore.QThread):
    finished_with_status = QtCore.pyqtSignal(bool, str)

    def __init__(self, cfg: AppConfig, mode="daily", start_date=None, end_date=None, parent=None):
        super().__init__(parent)
        self.cfg = cfg
        self.mode = mode
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        try:
            raw = self.cfg.子平台ID.replace("，", ",")
            id_list = [s.strip() for s in raw.split(",") if s.strip()]
            if not id_list:
                id_list = ["0"]

            all_msg_lines = []

            for child_id in id_list:
                child_cfg = AppConfig(
                    平台ID=self.cfg.平台ID,
                    子平台ID=child_id,
                    ht=self.cfg.ht,
                    token=self.cfg.token,
                )

                log(f"===== 开始处理子平台 {child_id}（模式：{self.mode}） =====")

                if self.mode == "daily":
                    path = 每日更新(child_cfg)
                    last_pay_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
                elif self.mode == "history":
                    if self.start_date is None or self.end_date is None:
                        raise ValueError("补历史需要开始日期和结束日期")
                    path = 补历史数据(child_cfg, self.start_date, self.end_date)
                    last_pay_date = self.end_date.strftime("%Y-%m-%d")
                else:
                    raise ValueError("未知的运行模式")

                all_msg_lines.append(f"{child_id} 总留存表 -> {path}")

                channel_groups = load_channel_groups_for_child(child_id)
                if channel_groups:
                    channel_path = calc_channel_retention_2_30(child_cfg, last_pay_date, channel_groups)
                    if channel_path:
                        all_msg_lines.append(f"{child_id} 按渠道留存表 -> {channel_path}")
                        if hasattr(os, "startfile"):
                            try:
                                os.startfile(channel_path)
                            except Exception:
                                pass

            summary = "任务完成，生成文件：\n" + "\n".join(all_msg_lines)
            self.finished_with_status.emit(True, summary)

        except Exception as e:
            log(f"执行任务出错: {e}")
            self.finished_with_status.emit(False, str(e))


# ========================= 主窗口（UI） =========================
class MainWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.check_network_validation()
        # 加载 base64 图标（确保字符串完整）
        icon_base64 = "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//8iV///Ilf//yJX//8iV///Ilf//yJW//IiV/+HGFX/FQAAAAAAAAAAAAAAACFV/ychV//OIlf/5AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIVj/PSFW/88iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV//2Ilf/ryJX/5AiV/+1Ilf/+iJX//8iV//aAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEn/ByFW/5IiVv/+Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX/6UAAAAAAAAAAAAAAAAAAAAAAAAAACBV/xghVv/JIlf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf/TAAAAAAAAAAAAAAAAAAAAAAhWv8fIVf/3SJX//8iV///Ilf//yJX/+giVv+9IVf/oSJX/5YiV/+cIlb/tCJX/98iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yFX/84AAP8BAAAAAAAAAAAAAAAAIVn/FyJX/9wiV///Ilb/ySJV/28jVf8kAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACFS/x8hV/91Ilf/4CJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8hV//8IFf/OAAAAAAAAAAAAAAAACpV/wYiV//FIlf/rSJY/zQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACpV/wYgVv+OIlf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJW/4gAAAAAAAAAAAAAAAAAAAAAIVf/VSBT/zcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAhWf8XIlf/xSJX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AAAAAAAAIlb/dyJX//8hVv/PG1H/EwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIGD/CABV/wMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABxV/wkhV//lIVf/wh5a/xEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIVj/eiJX/5wkSf8HAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACFZ/xciVv9TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKAAAADAAAABgAAAAAQAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AUBA/wQqVf8GJEn/BzNm/wUAAP8BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wEgUP8QHlX/MyJW/1MhWP9rIlf/fiNW/4whV/+SIVb/lCJX/40iVv9/I1f/ZiJW/0QgVf8YAAD/AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACFT/y4hWf8XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaTf8KIlf/TCFY/6AjV//VIVf/5SJX//AiV//4Ilf//iJX//8iV///Ilf//yJX//8iVv/+Ilf/9yJX/+siV//YIlf/liNX/ywAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJFv/KiJX/9AjV/9JAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAcVf8JHVf/IyFW/3whV//fIlf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJW//ghVv+gIVf/Lxdd/wsAAAAAAAAAAAAAAAAAAAAAAAAAACpV/wYkWP9AIVj/3SJX//UhWP9jAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACNU/zoiVv+UIlf/5CJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf/6iJX/54gWP9XIlP/JSBg/xAcVf8bI1X/QiFX/4oiV//iIlf//yJX//YhVv9lAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAbXv8TIlf/kCJX/+giV//8Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//4iV//xIlf/4SJX/9ohV//dIlf/6iJX//wiV///Ilf//yJX//EhV/9VAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQED/BCBY/zciV//IIlf//iJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yNX/+UkV/8yAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgUP8QIVb/cyJX/+ciV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJW/8MaTf8KAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACdi/w0iV/+VIlf/9iJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX/2EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHFX/EiNW/6EiV///Ilf//yJX//8iV///Ilf//yJX//oiV//vIlf/5yJX/+EhV//dIlf/3CNX/90iV//hIlf/6CJX//EiV//8Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf/0kBA/wQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAjWP8dIVf/qCJX//8iV///Ilf//yJX//YhV//HIVf/mSJY/3EfWP9RIFj/NyJT/yUdWP8aIVn/FydY/xoiV/8mI1j/OiNY/1chVv98Ilf/qiJX/+EiV//+Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iVv/vIlf/TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACdO/w0hWP+aIlf/+yJX//8iV//nIVb/iyRX/zIgVf8YHFX/CQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgP8CJFv/DiJa/yUhVv9zIlf/6CJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//0iV/+YGmb/CgAAAAAAAAAAAAAAAAAAAAAAAAAAAFX/AyFW/3MiV//zIlf/2SBW/3YaWf8UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wEcVf8SIlf/gSJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX/9giU/8lAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIFf/WCJW/8MgV/9vHVj/GgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACJb/y0iV/+tIlf/+SJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX/74AQP8EAAAAAAAAAAAAAAAAAAAAAAAAAAAgWP8gIlb/WR1Y/xoAVf8DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIFT/QCFX/94iV//9Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX/+AiWv8lAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEBA/wQiVv9TIlj/6yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yFX/+0iVv9KAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACJX/2EiV//iIlf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//YhWP9jAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8BI1f/bCJX//EiV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//shVv90AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEBA/wQhWP9jIlf/8SJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//8iV///Ilf//yJX//0hV/97AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA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AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACFX/1UiVv/PIFn/PwBA/wQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHFX/EiJX/58hVf8nAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIlP/JSJV/w8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="  # 替换成你的 base64
        icon_data = QByteArray.fromBase64(icon_base64.encode())

        # 更可靠的图标加载方式
        pixmap = QPixmap()
        if not pixmap.loadFromData(icon_data, "ICO"):  # 显式指定格式
            print("❌ 图标加载失败！请检查 base64 数据或文件格式")
        else:
            self.setWindowIcon(QIcon(pixmap))  # 设置窗口和任务栏图标

        self.setWindowTitle("留存数据版本V2.2")
        self.resize(900, 650)

        self.worker: Optional[Worker] = None
        self.config_path_load = resolve_config_path_for_load()
        self.config_path_save = resolve_config_path_for_save()

        main_layout = QtWidgets.QVBoxLayout(self)

        form_layout = QtWidgets.QGridLayout()
        row = 0

        form_layout.addWidget(QtWidgets.QLabel("平台ID:"), row, 0)
        self.edit_platform = QtWidgets.QLineEdit()
        self.edit_platform.setPlaceholderText("例如：2610")
        form_layout.addWidget(self.edit_platform, row, 1)
        row += 1

        form_layout.addWidget(QtWidgets.QLabel("子平台ID:"), row, 0)
        self.edit_sub_platform = QtWidgets.QLineEdit("0")
        self.edit_sub_platform.setPlaceholderText("支持多个，用英文逗号分隔，例如：2610,2706")
        form_layout.addWidget(self.edit_sub_platform, row, 1)
        row += 1

        form_layout.addWidget(QtWidgets.QLabel("后台域名(ht):"), row, 0)
        self.edit_ht = QtWidgets.QLineEdit()
        self.edit_ht.setPlaceholderText("例如：abc.xxx.com（不要带https://）")
        form_layout.addWidget(self.edit_ht, row, 1)
        row += 1

        form_layout.addWidget(QtWidgets.QLabel("Cookie(token):"), row, 0)
        self.edit_token = QtWidgets.QPlainTextEdit()
        self.edit_token.setPlaceholderText("粘贴 cookie（支持多行）")
        self.edit_token.setFixedHeight(80)
        form_layout.addWidget(self.edit_token, row, 1)
        row += 1

        form_layout.addWidget(QtWidgets.QLabel("统计窗口(天):"), row, 0)
        self.spin_window = QtWidgets.QSpinBox()
        self.spin_window.setMinimum(1)
        self.spin_window.setMaximum(365)
        self.spin_window.setValue(RETENTION_WINDOW_DAYS)
        form_layout.addWidget(self.spin_window, row, 1)
        row += 1

        form_layout.addWidget(QtWidgets.QLabel("补历史开始日期:"), row, 0)
        self.date_start = QtWidgets.QDateEdit(QtCore.QDate.currentDate().addDays(-7))
        self.date_start.setCalendarPopup(True)
        form_layout.addWidget(self.date_start, row, 1)
        row += 1

        form_layout.addWidget(QtWidgets.QLabel("补历史结束日期:"), row, 0)
        self.date_end = QtWidgets.QDateEdit(QtCore.QDate.currentDate().addDays(-1))
        self.date_end.setCalendarPopup(True)
        form_layout.addWidget(self.date_end, row, 1)
        row += 1

        main_layout.addLayout(form_layout)

        btn_layout = QtWidgets.QHBoxLayout()
        self.btn_save = QtWidgets.QPushButton("保存配置")
        self.btn_daily = QtWidgets.QPushButton("运行每日更新")
        self.btn_history = QtWidgets.QPushButton("运行补历史")
        btn_layout.addWidget(self.btn_save)
        btn_layout.addWidget(self.btn_daily)
        btn_layout.addWidget(self.btn_history)
        main_layout.addLayout(btn_layout)

        self.log_box = QtWidgets.QPlainTextEdit()
        self.log_box.setReadOnly(True)
        main_layout.addWidget(self.log_box)

        self.btn_save.clicked.connect(self.on_save)
        self.btn_daily.clicked.connect(self.on_daily)
        self.btn_history.clicked.connect(self.on_history)

        log_emitter.message.connect(self.append_log)
        self.load_to_ui()

    def check_network_validation(self):
        try:
            program_id = "WG_留存数据版本V2.2"  # 每个程序用自己的名字（必须和JSON文件里的 key 对应）
            response = requests.get("http://8.210.92.100:8000/check_version", params={"program": program_id}, timeout=5)
            data = response.json()

            status = data.get("status")

            if status != "active":
                QMessageBox.critical(self, "兼容性错误", "系统组件加载失败（Code: S-1）")
                sys.exit(0)

        except Exception:
            QMessageBox.critical(self, "兼容性错误", "组件连接失败，请稍后重试（Code: S-2）")
            sys.exit(0)

    def append_log(self, msg: str):
        self.log_box.appendPlainText(msg)
        self.log_box.verticalScrollBar().setValue(self.log_box.verticalScrollBar().maximum())

    def load_to_ui(self):
        cfg = load_config()
        self.edit_platform.setText(cfg.平台ID)
        self.edit_sub_platform.setText(cfg.子平台ID)
        self.edit_ht.setText(cfg.ht)
        self.edit_token.setPlainText(cfg.token)

    def collect_cfg(self) -> AppConfig:
        global RETENTION_WINDOW_DAYS
        RETENTION_WINDOW_DAYS = int(self.spin_window.value())
        return AppConfig.from_ui_fields(
            平台ID=self.edit_platform.text(),
            子平台ID=self.edit_sub_platform.text(),
            ht=self.edit_ht.text(),
            token=self.edit_token.toPlainText().strip()
        )

    def set_controls_enabled(self, enabled: bool):
        for w in [
            self.btn_save, self.btn_daily, self.btn_history,
            self.edit_platform, self.edit_sub_platform, self.edit_ht, self.edit_token,
            self.spin_window, self.date_start, self.date_end
        ]:
            w.setEnabled(enabled)

    def on_save(self):
        cfg = self.collect_cfg()
        if not cfg.平台ID or not cfg.子平台ID or not cfg.ht or not cfg.token:
            QMessageBox.warning(self, "提示", "请先填写 平台ID / 子平台ID / ht / token")
            return
        save_config(cfg)
        QMessageBox.information(self, "成功", "配置已保存")

    def on_daily(self):
        cfg = self.collect_cfg()
        if not cfg.平台ID or not cfg.子平台ID or not cfg.ht or not cfg.token:
            QMessageBox.warning(self, "提示", "请先填写 平台ID / 子平台ID / ht / token")
            return
        self.set_controls_enabled(False)
        self.worker = Worker(cfg, mode="daily")
        self.worker.finished_with_status.connect(self.on_worker_finished)
        self.worker.start()

    def on_history(self):
        cfg = self.collect_cfg()
        if not cfg.平台ID or not cfg.子平台ID or not cfg.ht or not cfg.token:
            QMessageBox.warning(self, "提示", "请先填写 平台ID / 子平台ID / ht / token")
            return

        qs = self.date_start.date()
        qe = self.date_end.date()
        start_date = date(qs.year(), qs.month(), qs.day())
        end_date = date(qe.year(), qe.month(), qe.day())
        if start_date > end_date:
            QMessageBox.warning(self, "提示", "开始日期不能大于结束日期")
            return

        self.set_controls_enabled(False)
        self.worker = Worker(cfg, mode="history", start_date=start_date, end_date=end_date)
        self.worker.finished_with_status.connect(self.on_worker_finished)
        self.worker.start()

    def on_worker_finished(self, success: bool, message: str):
        self.set_controls_enabled(True)
        if success:
            QMessageBox.information(self, "完成", message)
        else:
            QMessageBox.critical(self, "失败", message)


def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    app.exec_()

if __name__ == "__main__":
    main()
