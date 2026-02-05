import os
import io
import re
import datetime
import pandas as pd
import numpy as np
from flask import Flask, jsonify, request
from flask_cors import CORS
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================
app = Flask(__name__)
CORS(app)

DRIVE_FOLDER_ID = "1056rTo3LQ9vGhjUAJMEZLUCG98DJedRC"
SCOPES = ['https://www.googleapis.com/auth/drive'] 

REQUIRED_FILES = [
    "Ticket_Trong_Gio", "Ticket_Ngoai_Gio",
    "SLA_Ticket_Trong_Gio", "SLA_Ticket_Ngoai_Gio",
    "Zalo_Trong_Gio", "Zalo_Ngoai_Gio",
    "SLA_Zalo_Trong_Gio", "SLA_Zalo_Ngoai_Gio",
    "Miss_Hoi_Thoai", "Miss_Zalo", "Miss_Call",
    "Call_Den_Trong_Gio", "Call_Den_Ngoai_Gio",
    "Call_Di_Trong_Gio", "Call_Di_Ngoai_Gio"
]

DATA_CACHE = {}

# ==============================================================================
# 2. HÀM QUẢN LÝ DRIVE
# ==============================================================================
def get_drive_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def download_file_to_dataframe(service, file_name):
    print(f"   ⬇️ Đang tải: {file_name}...", end=" ")
    query = f"name = '{file_name}.parquet' and '{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])

    if files:
        file_id = files[0]['id']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        try:
            df = pd.read_parquet(fh)
            if 'Ngay_Cào' in df.columns:
                df['Ngay_Cào'] = pd.to_datetime(df['Ngay_Cào'], errors='coerce')
            print("✅ OK")
            return df
        except Exception as e:
            print(f"❌ Lỗi đọc file: {e}")
            return pd.DataFrame()
    print("⚠️ Không tìm thấy file")
    return pd.DataFrame()

def load_all_data():
    global DATA_CACHE
    service = get_drive_service()
    print("\n📦 Đang nạp đạn (Load Data từ Drive)...")
    for fname in REQUIRED_FILES:
        DATA_CACHE[fname] = download_file_to_dataframe(service, fname)
    print("🚀 Đã nạp xong toàn bộ dữ liệu!\n")

# ==============================================================================
# 3. HELPER FUNCTIONS
# ==============================================================================

def filter_by_date(df, start_date, end_date):
    if df.empty or 'Ngay_Cào' not in df.columns: return df
    mask = (df['Ngay_Cào'] >= start_date) & (df['Ngay_Cào'] <= end_date)
    return df.loc[mask].copy()

def filter_by_tag(df, tag_keyword, exclude=False):
    if df.empty or 'Tags' not in df.columns: 
        return df if exclude else pd.DataFrame()
    df['Tags'] = df['Tags'].fillna("")
    mask = df['Tags'].str.contains(tag_keyword, case=False, na=False)
    return df[~mask] if exclude else df[mask]

def filter_exclude_list(df, list_tags):
    if df.empty or 'Tags' not in df.columns: return df
    df['Tags'] = df['Tags'].fillna("")
    pattern = '|'.join([re.escape(t) for t in list_tags])
    return df[~df['Tags'].str.contains(pattern, case=False, na=False)]

def parse_duration_to_seconds(duration_str):
    if not isinstance(duration_str, str): return 0
    m = re.search(r'(\d+)\s*phút', duration_str)
    s = re.search(r'(\d+)\s*giây', duration_str)
    minutes = int(m.group(1)) if m else 0
    seconds = int(s.group(1)) if s else 0
    return minutes * 60 + seconds

def format_seconds(total_seconds):
    m = total_seconds // 60
    s = total_seconds % 60
    return f"{m}m {s}s"

def generate_scatter_points(df, date_range_labels):
    points = []
    if df.empty: return points
    date_map = {d_str: i for i, d_str in enumerate(date_range_labels)}
    time_col = 'Thời gian' if 'Thời gian' in df.columns else ('Thời gian tạo' if 'Thời gian tạo' in df.columns else None)
    if not time_col: return points

    for _, row in df.iterrows():
        try:
            if pd.isnull(row['Ngay_Cào']): continue
            ngay_cao = row['Ngay_Cào'].strftime('%Y-%m-%d')
            if ngay_cao in date_map:
                x_val = date_map[ngay_cao]
                dt = pd.to_datetime(str(row[time_col]), errors='coerce')
                if pd.notnull(dt):
                    y_val = dt.hour + dt.minute / 60.0
                    points.append({"x": x_val, "y": y_val})
        except: continue
    return points

def calculate_employee_stats(dfs_in, dfs_out, dfs_sla, filter_mode="ALL", filter_tag=""):
    stats = {}
    combined_in = pd.concat(dfs_in) if isinstance(dfs_in, list) else dfs_in
    combined_out = pd.concat(dfs_out) if isinstance(dfs_out, list) else dfs_out
    combined_sla = pd.concat(dfs_sla) if isinstance(dfs_sla, list) else dfs_sla

    if filter_mode == "EXCLUDE":
        combined_in = filter_by_tag(combined_in, filter_tag, exclude=True)
        combined_out = filter_by_tag(combined_out, filter_tag, exclude=True)
        combined_sla = filter_by_tag(combined_sla, filter_tag, exclude=True)
    elif filter_mode == "ONLY":
        combined_in = filter_by_tag(combined_in, filter_tag, exclude=False)
        combined_out = filter_by_tag(combined_out, filter_tag, exclude=False)
        combined_sla = filter_by_tag(combined_sla, filter_tag, exclude=False)

    def count_by_agent(df, metric_name):
        if df.empty or 'Nhân viên hệ thống' not in df.columns: return
        counts = df['Nhân viên hệ thống'].value_counts()
        for name, val in counts.items():
            if not name or str(name) == "nan": continue
            if name not in stats: stats[name] = {"name": name, "in": 0, "out": 0, "sla": 0}
            stats[name][metric_name] += int(val)

    count_by_agent(combined_in, "in")
    count_by_agent(combined_out, "out")
    count_by_agent(combined_sla, "sla")
    return list(stats.values())

def get_daily_counts(df_list, date_range):
    # Fix an toàn: Nếu df_list rỗng thì trả về 0 hết
    if not df_list: 
        return [0] * len(date_range)
    
    merged = pd.concat(df_list)
    counts = []
    for d in date_range:
        if merged.empty: counts.append(0)
        else: counts.append(len(merged[merged['Ngay_Cào'].dt.date == d.date()]))
    return counts

# === HÀM TÍNH TĂNG TRƯỞNG ===
def calc_growth(current, prev):
    if prev == 0:
        return 100 if current > 0 else 0
    return round(((current - prev) / prev) * 100, 1)

# ==============================================================================
# 4. API ENDPOINT (XỬ LÝ DYNAMIC DATE RANGE)
# ==============================================================================

@app.route('/api/get-group-data', methods=['GET'])
def get_group_data():
    s_arg = request.args.get('start')
    e_arg = request.args.get('end')
    
    if not s_arg or not e_arg: return jsonify({"error": "Thiếu ngày"}), 400

    if not DATA_CACHE: load_all_data()

    curr_start = pd.to_datetime(s_arg)
    curr_end = pd.to_datetime(e_arg)
    
    # Tính khoảng thời gian so sánh (Previous)
    duration = curr_end - curr_start
    prev_end = curr_start - datetime.timedelta(days=1)
    prev_start = prev_end - duration

    print(f"🔍 Current: {curr_start.date()} -> {curr_end.date()}")
    print(f"🔍 Previous: {prev_start.date()} -> {prev_end.date()}")

    # LỌC DATA
    db = {}      # Current
    db_prev = {} # Previous

    for key, df in DATA_CACHE.items():
        db[key] = filter_by_date(df, curr_start, curr_end)
        db_prev[key] = filter_by_date(df, prev_start, prev_end)

    # CHECK TRÙNG MISS CALL
    f_call_miss = db["Miss_Call"]
    if not f_call_miss.empty and 'SDT' in f_call_miss.columns:
        f_call_miss = f_call_miss.drop_duplicates(subset=['SDT']) 

    f_call_miss_prev = db_prev["Miss_Call"]
    if not f_call_miss_prev.empty and 'SDT' in f_call_miss_prev.columns:
        f_call_miss_prev = f_call_miss_prev.drop_duplicates(subset=['SDT'])

    # --- SHORTCUT VARS ---
    c_tick_in = db["Ticket_Trong_Gio"]
    c_tick_out = db["Ticket_Ngoai_Gio"]
    c_tick_sla = pd.concat([db["SLA_Ticket_Trong_Gio"], db["SLA_Ticket_Ngoai_Gio"]])
    c_tick_miss = db["Miss_Hoi_Thoai"]

    c_zalo_in = db["Zalo_Trong_Gio"]
    c_zalo_out = db["Zalo_Ngoai_Gio"]
    c_zalo_sla = pd.concat([db["SLA_Zalo_Trong_Gio"], db["SLA_Zalo_Ngoai_Gio"]])
    c_zalo_miss = db["Miss_Zalo"]

    c_call_in_in = db["Call_Den_Trong_Gio"]
    c_call_in_out = db["Call_Den_Ngoai_Gio"]
    c_call_out_in = db["Call_Di_Trong_Gio"]
    c_call_out_out = db["Call_Di_Ngoai_Gio"]

    # Prev Vars
    p_tick_in = db_prev["Ticket_Trong_Gio"]
    p_tick_out = db_prev["Ticket_Ngoai_Gio"]
    p_tick_sla = pd.concat([db_prev["SLA_Ticket_Trong_Gio"], db_prev["SLA_Ticket_Ngoai_Gio"]])
    p_tick_miss = db_prev["Miss_Hoi_Thoai"]

    p_zalo_in = db_prev["Zalo_Trong_Gio"]
    p_zalo_out = db_prev["Zalo_Ngoai_Gio"]
    p_zalo_sla = pd.concat([db_prev["SLA_Zalo_Trong_Gio"], db_prev["SLA_Zalo_Ngoai_Gio"]])
    p_zalo_miss = db_prev["Miss_Zalo"]
    
    # 5. TÍNH STATS
    
    # --- Helper mới (Fix lỗi crash khi list rỗng) ---
    def calc_section_stats(dfs_c, dfs_p, mode="ALL", tag=""):
        # Combine Current (An toàn)
        curr_all = pd.DataFrame()
        if isinstance(dfs_c, list) and dfs_c:
            curr_all = pd.concat(dfs_c)
        elif not isinstance(dfs_c, list):
            curr_all = dfs_c

        # Combine Prev (An toàn)
        prev_all = pd.DataFrame()
        if isinstance(dfs_p, list) and dfs_p:
            prev_all = pd.concat(dfs_p)
        elif not isinstance(dfs_p, list):
            prev_all = dfs_p
        
        # Filter
        if mode == "EXCLUDE":
            curr_all = filter_by_tag(curr_all, tag, exclude=True)
            prev_all = filter_by_tag(prev_all, tag, exclude=True)
        elif mode == "ONLY":
            curr_all = filter_by_tag(curr_all, tag, exclude=False)
            prev_all = filter_by_tag(prev_all, tag, exclude=False)
            
        val_curr = len(curr_all)
        val_prev = len(prev_all)
        return val_curr, calc_growth(val_curr, val_prev)

    # A. SUBIZ
    sb_in_v, sb_in_g = calc_section_stats([c_tick_in], [p_tick_in], "EXCLUDE", "MBHXH")
    sb_out_v, sb_out_g = calc_section_stats([c_tick_out], [p_tick_out], "EXCLUDE", "MBHXH")
    sb_miss_v, sb_miss_g = calc_section_stats([c_tick_miss], [p_tick_miss], "EXCLUDE", "MBHXH")
    sb_sla_v, sb_sla_g = calc_section_stats([c_tick_sla], [p_tick_sla], "EXCLUDE", "MBHXH")
    sb_total_v = sb_in_v + sb_out_v + sb_miss_v
    
    # Tính Total Prev thủ công (Không dùng hàm hacky nữa)
    p_sb_in_v = len(filter_by_tag(p_tick_in, "MBHXH", exclude=True))
    p_sb_out_v = len(filter_by_tag(p_tick_out, "MBHXH", exclude=True))
    p_sb_miss_v = len(filter_by_tag(p_tick_miss, "MBHXH", exclude=True))
    sb_total_g = calc_growth(sb_total_v, p_sb_in_v + p_sb_out_v + p_sb_miss_v)

    subiz_dashboard_stats = {
        "total": sb_total_v, "total_g": sb_total_g,
        "in": sb_in_v, "in_g": sb_in_g,
        "out": sb_out_v, "out_g": sb_out_g,
        "miss": sb_miss_v, "miss_g": sb_miss_g,
        "sla": sb_sla_v, "sla_g": sb_sla_g
    }

    # B. ZALO
    zl_in_v, zl_in_g = calc_section_stats([c_zalo_in], [p_zalo_in], "EXCLUDE", "MBHXH")
    zl_out_v, zl_out_g = calc_section_stats([c_zalo_out], [p_zalo_out], "EXCLUDE", "MBHXH")
    zl_miss_v, zl_miss_g = calc_section_stats([c_zalo_miss], [p_zalo_miss], "EXCLUDE", "MBHXH")
    zl_sla_v, zl_sla_g = calc_section_stats([c_zalo_sla], [p_zalo_sla], "EXCLUDE", "MBHXH")
    
    p_zl_in_v = len(filter_by_tag(p_zalo_in, "MBHXH", exclude=True))
    p_zl_out_v = len(filter_by_tag(p_zalo_out, "MBHXH", exclude=True))
    p_zl_miss_v = len(filter_by_tag(p_zalo_miss, "MBHXH", exclude=True))
    
    zl_total_v = zl_in_v + zl_out_v + zl_miss_v
    zl_total_g = calc_growth(zl_total_v, p_zl_in_v + p_zl_out_v + p_zl_miss_v)

    zalo_dashboard_stats = {
        "total": zl_total_v, "total_g": zl_total_g,
        "in": zl_in_v, "in_g": zl_in_g,
        "out": zl_out_v, "out_g": zl_out_g,
        "miss": zl_miss_v, "miss_g": zl_miss_g,
        "sla": zl_sla_v, "sla_g": zl_sla_g
    }

    # C. BHXH
    bh_in_v, bh_in_g = calc_section_stats([c_tick_in, c_zalo_in], [p_tick_in, p_zalo_in], "ONLY", "MBHXH")
    bh_out_v, bh_out_g = calc_section_stats([c_tick_out, c_zalo_out], [p_tick_out, p_zalo_out], "ONLY", "MBHXH")
    bh_miss_v, bh_miss_g = calc_section_stats([c_tick_miss, c_zalo_miss], [p_tick_miss, p_zalo_miss], "ONLY", "MBHXH")
    bh_sla_v, bh_sla_g = calc_section_stats([c_tick_sla, c_zalo_sla], [p_tick_sla, p_zalo_sla], "ONLY", "MBHXH")
    
    p_bh_in_v = len(filter_by_tag(pd.concat([p_tick_in, p_zalo_in]), "MBHXH", exclude=False))
    p_bh_out_v = len(filter_by_tag(pd.concat([p_tick_out, p_zalo_out]), "MBHXH", exclude=False))
    p_bh_miss_v = len(filter_by_tag(pd.concat([p_tick_miss, p_zalo_miss]), "MBHXH", exclude=False))

    bh_total_v = bh_in_v + bh_out_v + bh_miss_v
    bh_total_g = calc_growth(bh_total_v, p_bh_in_v + p_bh_out_v + p_bh_miss_v)

    bhxh_dashboard_stats = {
        "total": bh_total_v, "total_g": bh_total_g,
        "in": bh_in_v, "in_g": bh_in_g,
        "out": bh_out_v, "out_g": bh_out_g,
        "miss": bh_miss_v, "miss_g": bh_miss_g,
        "sla": bh_sla_v, "sla_g": bh_sla_g
    }

    # --- SẢN PHẨM & BIỂU ĐỒ ---
    products_config = [
        {"id": "subiz1", "name": "SUBIZ 1.0", "tag": "EINVOICE1.0", "color": "#00e396"},
        {"id": "subiz2", "name": "SUBIZ 2.0", "tag": "EINVOICE2.0", "color": "#008ffb"},
        {"id": "smi", "name": "SMI (HÓA ĐƠN)", "tag": "MSMI", "color": "#feb019"},
        {"id": "bhxh", "name": "HỖ TRỢ BHXH", "tag": "MBHXH", "color": "#ff4560"},
        {"id": "thue", "name": "MCTTNCN (THUẾ)", "tag": "MTNCN", "color": "#775dd0"},
        {"id": "cks", "name": "HỖ TRỢ CKS", "tag": "CKS", "color": "#3f51b5"},
        {"id": "m2sale", "name": "M2SALE", "tag": "M2SALE", "color": "#00bcd4"},
        {"id": "mseller", "name": "MSELLER", "tag": "MSELLER", "color": "#4caf50"},
        {"id": "mtax", "name": "MTAX", "tag": "MTAX", "color": "#cddc39"},
    ]
    
    defined_tags = [p["tag"] for p in products_config]
    channels_data = []
    daily_by_product = {}
    date_labels = [d.strftime('%Y-%m-%d') for d in pd.date_range(curr_start, curr_end)]

    for prod in products_config:
        tag = prod["tag"]
        dfs_in = [filter_by_tag(c_tick_in, tag), filter_by_tag(c_zalo_in, tag)]
        dfs_out = [filter_by_tag(c_tick_out, tag), filter_by_tag(c_zalo_out, tag)]
        dfs_miss = [filter_by_tag(c_tick_miss, tag), filter_by_tag(c_zalo_miss, tag)]
        df_sla = filter_by_tag(pd.concat([c_tick_sla, c_zalo_sla]), tag)
        
        # Growth
        total_curr = sum(len(d) for d in dfs_in + dfs_out + dfs_miss)
        
        p_dfs_in = [filter_by_tag(p_tick_in, tag), filter_by_tag(p_zalo_in, tag)]
        p_dfs_out = [filter_by_tag(p_tick_out, tag), filter_by_tag(p_zalo_out, tag)]
        p_dfs_miss = [filter_by_tag(p_tick_miss, tag), filter_by_tag(p_zalo_miss, tag)]
        total_prev = sum(len(d) for d in p_dfs_in + p_dfs_out + p_dfs_miss)
        
        growth = calc_growth(total_curr, total_prev)

        channels_data.append({
            "id": prod["id"], "name": prod["name"], "type": "ticket", "color": prod["color"],
            "in": sum(len(d) for d in dfs_in), "out": sum(len(d) for d in dfs_out),
            "miss": sum(len(d) for d in dfs_miss), "sla": len(df_sla),
            "growth": growth
        })
        daily_by_product[prod["id"]] = get_daily_counts(dfs_in + dfs_out + dfs_miss, pd.date_range(curr_start, curr_end))

    # SẢN PHẨM KHÁC
    o_in = [filter_exclude_list(c_tick_in, defined_tags), filter_exclude_list(c_zalo_in, defined_tags)]
    o_out = [filter_exclude_list(c_tick_out, defined_tags), filter_exclude_list(c_zalo_out, defined_tags)]
    o_miss = [filter_exclude_list(c_tick_miss, defined_tags), filter_exclude_list(c_zalo_miss, defined_tags)]
    o_sla = filter_exclude_list(pd.concat([c_tick_sla, c_zalo_sla]), defined_tags)
    
    # Prev Others
    po_in = [filter_exclude_list(p_tick_in, defined_tags), filter_exclude_list(p_zalo_in, defined_tags)]
    po_out = [filter_exclude_list(p_tick_out, defined_tags), filter_exclude_list(p_zalo_out, defined_tags)]
    po_miss = [filter_exclude_list(p_tick_miss, defined_tags), filter_exclude_list(p_zalo_miss, defined_tags)]
    
    total_other_curr = sum(len(d) for d in o_in + o_out + o_miss)
    total_other_prev = sum(len(d) for d in po_in + po_out + po_miss)

    channels_data.append({
        "id": "others", "name": "SẢN PHẨM KHÁC", "type": "ticket", "color": "#546E7A",
        "in": sum(len(d) for d in o_in), "out": sum(len(d) for d in o_out),
        "miss": sum(len(d) for d in o_miss), "sla": len(o_sla),
        "growth": calc_growth(total_other_curr, total_other_prev)
    })
    daily_by_product["others"] = get_daily_counts(o_in + o_out + o_miss, pd.date_range(curr_start, curr_end))

    # TỔNG ĐÀI
    dur_in = sum(df['Thời lượng'].apply(parse_duration_to_seconds).sum() for df in [c_call_in_in, c_call_in_out])
    dur_out = sum(df['Thời lượng'].apply(parse_duration_to_seconds).sum() for df in [c_call_out_in, c_call_out_out])
    
    channels_data.append({"id": "call_in", "name": "GỌI VÀO", "type": "call_in", "count_in": len(c_call_in_in), "count_out": len(c_call_in_out), "miss": len(f_call_miss), "duration": format_seconds(dur_in), "color": "#008ffb"})
    channels_data.append({"id": "call_out", "name": "GỌI RA", "type": "call_out", "count_in": len(c_call_out_in), "count_out": len(c_call_out_out), "miss": 0, "duration": format_seconds(dur_out), "color": "#feb019"})

    # --- SCATTER ---
    sIII_miss = filter_by_tag(c_tick_miss, "MBHXH", exclude=True)
    sIII_sla = filter_by_tag(c_tick_sla, "MBHXH", exclude=True)
    
    sIV_miss = filter_by_tag(c_zalo_miss, "MBHXH", exclude=True)
    sIV_sla = filter_by_tag(c_zalo_sla, "MBHXH", exclude=True)
    
    sV_miss = filter_by_tag(pd.concat([c_tick_miss, c_zalo_miss]), "MBHXH", exclude=False)
    sV_sla = filter_by_tag(pd.concat([c_tick_sla, c_zalo_sla]), "MBHXH", exclude=False)

    # --- NHÂN VIÊN ---
    emp_subiz = calculate_employee_stats([c_tick_in], [c_tick_out], [c_tick_sla], "EXCLUDE", "MBHXH")
    emp_zalo = calculate_employee_stats([c_zalo_in], [c_zalo_out], [c_zalo_sla], "EXCLUDE", "MBHXH")
    emp_bhxh = calculate_employee_stats([c_tick_in, c_zalo_in], [c_tick_out, c_zalo_out], [c_tick_sla, c_zalo_sla], "ONLY", "MBHXH")

    # --- RATE ---
    total_calls = len(c_call_in_in) + len(c_call_in_out) + len(f_call_miss)
    rate = ((len(c_call_in_in) + len(c_call_in_out)) / total_calls * 100) if total_calls > 0 else 0

    response_data = {
        "channels": channels_data,
        "employees_subiz": emp_subiz,
        "employees_zalo": emp_zalo,
        "employees_bhxh": emp_bhxh,
        "charts": {
            "dates": date_labels,
            "daily_by_product": daily_by_product,
            "scatter_subiz_miss": generate_scatter_points(sIII_miss, date_labels),
            "scatter_subiz_sla": generate_scatter_points(sIII_sla, date_labels),
            "scatter_zalo_miss": generate_scatter_points(sIV_miss, date_labels),
            "scatter_zalo_sla": generate_scatter_points(sIV_sla, date_labels),
            "scatter_bhxh_miss": generate_scatter_points(sV_miss, date_labels),
            "scatter_bhxh_sla": generate_scatter_points(sV_sla, date_labels),
            
            "daily_ticket_in": get_daily_counts([c_tick_in], pd.date_range(curr_start, curr_end)),
            "daily_ticket_out": get_daily_counts([c_tick_out], pd.date_range(curr_start, curr_end)),
            "daily_ticket_sla": get_daily_counts([c_tick_sla], pd.date_range(curr_start, curr_end)),
            "daily_call_in": get_daily_counts([c_call_in_in, c_call_in_out], pd.date_range(curr_start, curr_end)),
            "daily_call_out": get_daily_counts([c_call_out_in, c_call_out_out], pd.date_range(curr_start, curr_end)),
            
             "call_traffic": {
                "in": get_daily_counts([c_call_in_in, c_call_in_out], pd.date_range(curr_start, curr_end)),
                "out": get_daily_counts([c_call_out_in, c_call_out_out], pd.date_range(curr_start, curr_end)),
                "miss": get_daily_counts([f_call_miss], pd.date_range(curr_start, curr_end))
            }
        },
        "growth_stats": {
            "subiz": subiz_dashboard_stats,
            "zalo": zalo_dashboard_stats,
            "bhxh": bhxh_dashboard_stats
        },
        "zalo_stats": {
             "in": len(filter_by_tag(c_zalo_in, "MBHXH", exclude=True)),
             "out": len(filter_by_tag(c_zalo_out, "MBHXH", exclude=True)),
             "miss": len(filter_by_tag(c_zalo_miss, "MBHXH", exclude=True)),
             "sla": len(filter_by_tag(c_zalo_sla, "MBHXH", exclude=True))
        },
        "call_stats": {"rate": f"{rate:.1f}%", "avg_duration": format_seconds(dur_in)}
    }
    return jsonify(response_data)

if __name__ == '__main__':
    try: load_all_data()
    except Exception as e: print(f"⚠️ Chưa load được data: {e}")
    print("🚀 API ĐANG CHẠY TẠI PORT 5000...")
    app.run(debug=True, port=5000)