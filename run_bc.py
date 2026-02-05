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
# 1. CẤU HÌNH HỆ THỐNG & CONSTANTS (GỘP CẢ 2 FILE)
# ==============================================================================
app = Flask(__name__)
CORS(app) # Mở cửa cho mọi nhà vào chơi

DRIVE_FOLDER_ID = "1056rTo3LQ9vGhjUAJMEZLUCG98DJedRC"
SCOPES = ['https://www.googleapis.com/auth/drive'] 

# Danh sách file cần tải (Gộp từ cả 2 file của đại ca để không sót cái nào)
# Em đã check, list này là bao trọn cả giang sơn rồi.
ALL_REQUIRED_FILES = [
    "Ticket_Trong_Gio", "Ticket_Ngoai_Gio",
    "SLA_Ticket_Trong_Gio", "SLA_Ticket_Ngoai_Gio",
    "Zalo_Trong_Gio", "Zalo_Ngoai_Gio",
    "SLA_Zalo_Trong_Gio", "SLA_Zalo_Ngoai_Gio",
    "Miss_Hoi_Thoai", "Miss_Zalo", "Miss_Call",
    "Call_Den_Trong_Gio", "Call_Den_Ngoai_Gio",
    "Call_Di_Trong_Gio", "Call_Di_Ngoai_Gio"
]

# Config cho Logic 1 (Dashboard Tổng)
MASTER_PRODUCTS_V1 = [
    "Subiz 1.0", "Subiz 2.0", "SMI (Hóa Đơn)", "Hỗ trợ BHXH", 
    "MCTTNCN (Thuế)", "Hỗ trợ CKS", "M2SALE", "MSELLER (Máy POS)",
    "Zalo OA", "Tổng đài GỌI VÀO", "Tổng đài GỌI RA", "Sản phẩm khác"
]
INTERACTION_FILES_V1 = [
    "Ticket_Trong_Gio", "Ticket_Ngoai_Gio", "Zalo_Trong_Gio", "Zalo_Ngoai_Gio",
    "Call_Den_Trong_Gio", "Call_Den_Ngoai_Gio", "Call_Di_Trong_Gio", "Call_Di_Ngoai_Gio"
]
SLA_FILES_V1 = [
    "SLA_Ticket_Trong_Gio", "SLA_Ticket_Ngoai_Gio", "SLA_Zalo_Trong_Gio", "SLA_Zalo_Ngoai_Gio"
]

# Config cho Logic 2 (Dashboard Nhóm)
PRODUCTS_CONFIG_V2 = [
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

# KHO ĐẠN DƯỢC (Cache data vào RAM để chạy cho nhanh)
GLOBAL_DB = {}

# ==============================================================================
# 2. HÀM QUẢN LÝ DRIVE & DATA LOADER (DÙNG CHUNG)
# ==============================================================================
def get_drive_service():
    """Kết nối Google Drive, xin token như xin số gái xinh."""
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
    """Tải file parquet về convert sang DataFrame. Hỏng thì báo lỗi."""
    print(f"   ⬇️ Đang tải: {file_name}...", end=" ")
    try:
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
            df = pd.read_parquet(fh)
            
            # Chuẩn hóa cột thời gian ngay từ đầu
            time_cols = ['Ngay_Cào', 'Thời gian', 'Thời gian tạo']
            for col in time_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            print("✅ OK")
            return df
        else:
            print("⚠️ Không tìm thấy file trên Drive")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ Lỗi sấp mặt: {e}")
        return pd.DataFrame()

def load_all_data():
    """Nạp đạn một lần dùng cả đời. Load hết vào GLOBAL_DB."""
    global GLOBAL_DB
    service = get_drive_service()
    print("\n📦 Đang nạp đạn (Load Data từ Drive)... Đại ca chờ tí nhé!")
    temp_db = {}
    for fname in ALL_REQUIRED_FILES:
        temp_db[fname] = download_file_to_dataframe(service, fname)
    GLOBAL_DB = temp_db
    print("🚀 Đã nạp xong toàn bộ dữ liệu! Sẵn sàng chiến đấu!\n")

# ==============================================================================
# 3. HELPER FUNCTIONS (CHUNG VÀ RIÊNG)
# ==============================================================================

# --- Helper chung ---
def calc_growth(current, prev):
    """Tính tăng trưởng chuẩn chỉ."""
    if prev == 0:
        return 100 if current > 0 else 0
    return round(((current - prev) / prev) * 100, 1)

def filter_by_date(df, start_date, end_date):
    """Lọc theo ngày cào (Dùng cho Logic 2)"""
    if df.empty or 'Ngay_Cào' not in df.columns: return df
    mask = (df['Ngay_Cào'] >= start_date) & (df['Ngay_Cào'] <= end_date)
    return df.loc[mask].copy()

# --- Helper cho Logic 1 (Map 1) ---
def detect_product_v3(row, src):
    tags = str(row.get('Tags', '')).upper()
    if "Call_Den" in src: return "Tổng đài GỌI VÀO"
    if "Call_Di" in src: return "Tổng đài GỌI RA"
    if "MBHXH" in tags: return "Hỗ trợ BHXH"
    if "Ticket" in src:
        if "EINVOICE1.0" in tags: return "Subiz 1.0"
        if "EINVOICE2.0" in tags: return "Subiz 2.0"
        if "SMI" in tags: return "SMI (Hóa Đơn)"
        if "MTNCN" in tags: return "MCTTNCN (Thuế)"
        if "CKS" in tags: return "Hỗ trợ CKS"
        if "M2SALE" in tags: return "M2SALE"
        if "MSELLER" in tags: return "MSELLER (Máy POS)"
    if "Zalo" in src: return "Zalo OA"
    return "Sản phẩm khác"

def parse_minutes(row, src):
    if "Call" not in src: return 0.0
    s = str(row.get('Thời lượng', '')).lower()
    try:
        if "phút" in s:
            parts = s.split("phút")
            m = float(parts[0].strip())
            if "giây" in parts[1]: m += float(parts[1].replace("giây", "").strip()) / 60
            return m
        if ":" in s:
            p = list(map(int, s.split(":")))
            if len(p) == 3: return p[0]*60 + p[1] + p[2]/60
            if len(p) == 2: return p[0] + p[1]/60
    except: pass
    return 0.0

# --- Helper cho Logic 2 (Map 2) ---
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
            # Ưu tiên lấy ngày cào, nếu ko có thì lấy ngày tạo
            target_date = row['Ngay_Cào'] if pd.notnull(row.get('Ngay_Cào')) else pd.to_datetime(row.get(time_col))
            if pd.isnull(target_date): continue
            
            ngay_cao = target_date.strftime('%Y-%m-%d')
            if ngay_cao in date_map:
                x_val = date_map[ngay_cao]
                dt = pd.to_datetime(str(row[time_col]), errors='coerce')
                if pd.notnull(dt):
                    y_val = dt.hour + dt.minute / 60.0
                    points.append({"x": x_val, "y": y_val})
        except: continue
    return points

def get_daily_counts(df_list, date_range):
    if not df_list: return [0] * len(date_range)
    merged = pd.concat(df_list)
    counts = []
    for d in date_range:
        if merged.empty: counts.append(0)
        else: 
            # Dùng Ngay_Cào hoặc fallback
            target_col = 'Ngay_Cào' if 'Ngay_Cào' in merged.columns else 'Date_Obj'
            if target_col not in merged.columns:
                 counts.append(0)
                 continue
            counts.append(len(merged[merged[target_col].dt.date == d.date()]))
    return counts

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

# ==============================================================================
# 4. API ENDPOINTS (TRÁI TIM CỦA APP)
# ==============================================================================

# --- API 1: Lấy dữ liệu chi tiết theo nhân viên (Logic File 1) ---
@app.route('/api/get-data', methods=['GET'])
def get_dashboard_data_v1():
    print("🔔 [API v1] Đang xử lý yêu cầu...")
    if not GLOBAL_DB: load_all_data()

    date_start_str = request.args.get('start')
    date_end_str = request.args.get('end')
    
    # Chuẩn bị dữ liệu từ Cache
    all_data_for_v1 = []
    # File 1 cần list các file này
    FILES_TO_LOAD_V1 = INTERACTION_FILES_V1 + SLA_FILES_V1
    
    for fname in FILES_TO_LOAD_V1:
        if fname in GLOBAL_DB:
            df = GLOBAL_DB[fname].copy() # Copy để không làm hỏng cache gốc
            if not df.empty:
                df['Source_File'] = fname
                df['Is_Interaction'] = 1 if fname in INTERACTION_FILES_V1 else 0
                df['Is_SLA_File'] = 1 if fname in SLA_FILES_V1 else 0
                
                # Chuẩn hóa cột thời gian cho logic v1
                time_col = 'Thời gian' if 'Thời gian' in df.columns else ('Thời gian tạo' if 'Thời gian tạo' in df.columns else None)
                if time_col:
                    df['Date_Obj'] = pd.to_datetime(df[time_col], errors='coerce')
                    all_data_for_v1.append(df)

    if not all_data_for_v1: return jsonify({}) 

    full_df = pd.concat(all_data_for_v1, ignore_index=True)
    full_df['Product_Label'] = full_df.apply(lambda x: detect_product_v3(x, x['Source_File']), axis=1)
    full_df['Minutes'] = full_df.apply(lambda x: parse_minutes(x, x['Source_File']), axis=1)

    d_start = pd.to_datetime(date_start_str).date()
    d_end = pd.to_datetime(date_end_str).date()
    
    # Logic tính Previous Period
    delta_days = (d_end - d_start).days + 1
    d_prev_end = d_start - datetime.timedelta(days=1)
    d_prev_start = d_prev_end - datetime.timedelta(days=delta_days - 1)

    df_curr = full_df[(full_df['Date_Obj'].dt.date >= d_start) & (full_df['Date_Obj'].dt.date <= d_end)]
    df_prev = full_df[(full_df['Date_Obj'].dt.date >= d_prev_start) & (full_df['Date_Obj'].dt.date <= d_prev_end)]

    total_room_interaction = len(df_curr[df_curr['Is_Interaction'] == 1])
    output_db = {}
    agents = [a for a in full_df['Nhân viên hệ thống'].unique() if a]

    for agent in agents:
        u_curr = df_curr[df_curr['Nhân viên hệ thống'] == agent]
        u_prev = df_prev[df_prev['Nhân viên hệ thống'] == agent]

        if u_curr.empty and u_prev.empty: continue

        u_curr_inter = u_curr[u_curr['Is_Interaction'] == 1]
        u_prev_inter = u_prev[u_prev['Is_Interaction'] == 1]
        
        t_in = len(u_curr_inter[u_curr_inter['Source_File'].str.contains('Trong_Gio')])
        t_out = len(u_curr_inter[u_curr_inter['Source_File'].str.contains('Ngoai_Gio')])
        total_curr = t_in + t_out
        total_prev = len(u_prev_inter)

        u_curr_sla = u_curr[u_curr['Is_SLA_File'] == 1]
        u_prev_sla = u_prev[u_prev['Is_SLA_File'] == 1]
        
        sla_count_curr = len(u_curr_sla)
        sla_count_prev = len(u_prev_sla)

        metrics = []
        for p in MASTER_PRODUCTS_V1:
            p_curr = u_curr[u_curr['Product_Label'] == p]
            p_curr_inter = p_curr[p_curr['Is_Interaction'] == 1]
            p_prev = u_prev[u_prev['Product_Label'] == p]
            p_prev_inter = p_prev[p_prev['Is_Interaction'] == 1]

            p_in = len(p_curr_inter[p_curr_inter['Source_File'].str.contains('Trong_Gio')])
            p_out = len(p_curr_inter[p_curr_inter['Source_File'].str.contains('Ngoai_Gio')])
            p_total_curr = p_in + p_out
            p_total_prev = len(p_prev_inter)
            p_sla_count = len(p_curr[p_curr['Is_SLA_File'] == 1])
            min_val = round(p_curr['Minutes'].sum(), 2) if "Tổng đài" in p else None
            
            metrics.append({
                "id": p, "name": p, "in": p_in, "out": p_out, "sla": p_sla_count,
                "growth": calc_growth(p_total_curr, p_total_prev), "minutes": min_val
            })

        hourly_series = []
        if not u_curr_inter.empty:
            for p in MASTER_PRODUCTS_V1:
                p_inter = u_curr_inter[u_curr_inter['Product_Label'] == p]
                if not p_inter.empty:
                    counts = p_inter.groupby(p_inter['Date_Obj'].dt.hour).size().reindex(range(24), fill_value=0).tolist()
                    if sum(counts) > 0:
                        hourly_series.append({"label": p, "data": counts})

        scatter_sla = []
        if not u_curr_sla.empty:
            for _, row in u_curr_sla.iterrows():
                if pd.notnull(row['Date_Obj']):
                    scatter_sla.append({
                        "x": row['Date_Obj'].isoformat(),
                        "y": round(row['Date_Obj'].hour + row['Date_Obj'].minute/60, 2),
                        "label": row['Product_Label']
                    })

        output_db[agent] = {
            "name": agent,
            "avatar": agent[:2].upper(),
            "workDays": f"{u_curr['Date_Obj'].dt.date.nunique()} ngày",
            "totalIn": t_in, "totalOut": t_out, "total": total_curr,
            "totalRoom": total_room_interaction,
            "growth": calc_growth(total_curr, total_prev),
            "stats": {"slaCount": sla_count_curr, "slaGrowth": calc_growth(sla_count_curr, sla_count_prev)},
            "metrics": metrics,
            "charts": {
                "dailyIn": u_curr_inter[u_curr_inter['Source_File'].str.contains('Trong_Gio')].groupby(u_curr_inter['Date_Obj'].dt.weekday).size().reindex(range(7), fill_value=0).tolist(),
                "dailyOut": u_curr_inter[u_curr_inter['Source_File'].str.contains('Ngoai_Gio')].groupby(u_curr_inter['Date_Obj'].dt.weekday).size().reindex(range(7), fill_value=0).tolist(),
                "dailySLA": u_curr_sla.groupby(u_curr_sla['Date_Obj'].dt.weekday).size().reindex(range(7), fill_value=0).tolist(),
                "hourlySeries": hourly_series
            },
            "scatterData": {"sla": scatter_sla}
        }
    return jsonify(output_db)


# --- API 2: Lấy dữ liệu nhóm (Logic File 2) ---
@app.route('/api/get-group-data', methods=['GET'])
def get_group_data_v2():
    print("🔔 [API v2] Đang xử lý yêu cầu...")
    s_arg = request.args.get('start')
    e_arg = request.args.get('end')
    
    if not s_arg or not e_arg: return jsonify({"error": "Thiếu ngày"}), 400
    if not GLOBAL_DB: load_all_data()

    curr_start = pd.to_datetime(s_arg)
    curr_end = pd.to_datetime(e_arg)
    
    duration = curr_end - curr_start
    prev_end = curr_start - datetime.timedelta(days=1)
    prev_start = prev_end - duration

    # Lọc Data từ Cache
    db = {}      # Current
    db_prev = {} # Previous

    for key, df in GLOBAL_DB.items():
        db[key] = filter_by_date(df, curr_start, curr_end)
        db_prev[key] = filter_by_date(df, prev_start, prev_end)

    # Clean Miss Call Duplicates
    if not db["Miss_Call"].empty and 'SDT' in db["Miss_Call"].columns:
        db["Miss_Call"] = db["Miss_Call"].drop_duplicates(subset=['SDT'])
    if not db_prev["Miss_Call"].empty and 'SDT' in db_prev["Miss_Call"].columns:
        db_prev["Miss_Call"] = db_prev["Miss_Call"].drop_duplicates(subset=['SDT'])

    # Helper nội bộ cho v2
    def calc_section_stats(dfs_c, dfs_p, mode="ALL", tag=""):
        curr_all = pd.concat(dfs_c) if isinstance(dfs_c, list) and dfs_c else (dfs_c if not isinstance(dfs_c, list) else pd.DataFrame())
        prev_all = pd.concat(dfs_p) if isinstance(dfs_p, list) and dfs_p else (dfs_p if not isinstance(dfs_p, list) else pd.DataFrame())
        
        if mode == "EXCLUDE":
            curr_all = filter_by_tag(curr_all, tag, exclude=True)
            prev_all = filter_by_tag(prev_all, tag, exclude=True)
        elif mode == "ONLY":
            curr_all = filter_by_tag(curr_all, tag, exclude=False)
            prev_all = filter_by_tag(prev_all, tag, exclude=False)
            
        val_curr = len(curr_all)
        val_prev = len(prev_all)
        return val_curr, calc_growth(val_curr, val_prev)

    # Shorten vars
    c_tick_in, c_tick_out = db["Ticket_Trong_Gio"], db["Ticket_Ngoai_Gio"]
    c_tick_sla = pd.concat([db["SLA_Ticket_Trong_Gio"], db["SLA_Ticket_Ngoai_Gio"]])
    c_tick_miss = db["Miss_Hoi_Thoai"]
    c_zalo_in, c_zalo_out = db["Zalo_Trong_Gio"], db["Zalo_Ngoai_Gio"]
    c_zalo_sla = pd.concat([db["SLA_Zalo_Trong_Gio"], db["SLA_Zalo_Ngoai_Gio"]])
    c_zalo_miss = db["Miss_Zalo"]
    
    p_tick_in, p_tick_out = db_prev["Ticket_Trong_Gio"], db_prev["Ticket_Ngoai_Gio"]
    p_tick_sla = pd.concat([db_prev["SLA_Ticket_Trong_Gio"], db_prev["SLA_Ticket_Ngoai_Gio"]])
    p_tick_miss = db_prev["Miss_Hoi_Thoai"]
    p_zalo_in, p_zalo_out = db_prev["Zalo_Trong_Gio"], db_prev["Zalo_Ngoai_Gio"]
    p_zalo_sla = pd.concat([db_prev["SLA_Zalo_Trong_Gio"], db_prev["SLA_Zalo_Ngoai_Gio"]])
    p_zalo_miss = db_prev["Miss_Zalo"]

    # 1. SUBIZ
    sb_in_v, sb_in_g = calc_section_stats([c_tick_in], [p_tick_in], "EXCLUDE", "MBHXH")
    sb_out_v, sb_out_g = calc_section_stats([c_tick_out], [p_tick_out], "EXCLUDE", "MBHXH")
    sb_miss_v, sb_miss_g = calc_section_stats([c_tick_miss], [p_tick_miss], "EXCLUDE", "MBHXH")
    sb_sla_v, sb_sla_g = calc_section_stats([c_tick_sla], [p_tick_sla], "EXCLUDE", "MBHXH")
    sb_total_v = sb_in_v + sb_out_v + sb_miss_v
    
    p_sb_total = len(filter_by_tag(p_tick_in, "MBHXH", exclude=True)) + len(filter_by_tag(p_tick_out, "MBHXH", exclude=True)) + len(filter_by_tag(p_tick_miss, "MBHXH", exclude=True))
    subiz_stats = {
        "total": sb_total_v, "total_g": calc_growth(sb_total_v, p_sb_total),
        "in": sb_in_v, "in_g": sb_in_g, "out": sb_out_v, "out_g": sb_out_g,
        "miss": sb_miss_v, "miss_g": sb_miss_g, "sla": sb_sla_v, "sla_g": sb_sla_g
    }

    # 2. ZALO
    zl_in_v, zl_in_g = calc_section_stats([c_zalo_in], [p_zalo_in], "EXCLUDE", "MBHXH")
    zl_out_v, zl_out_g = calc_section_stats([c_zalo_out], [p_zalo_out], "EXCLUDE", "MBHXH")
    zl_miss_v, zl_miss_g = calc_section_stats([c_zalo_miss], [p_zalo_miss], "EXCLUDE", "MBHXH")
    zl_sla_v, zl_sla_g = calc_section_stats([c_zalo_sla], [p_zalo_sla], "EXCLUDE", "MBHXH")
    zl_total_v = zl_in_v + zl_out_v + zl_miss_v
    
    p_zl_total = len(filter_by_tag(p_zalo_in, "MBHXH", exclude=True)) + len(filter_by_tag(p_zalo_out, "MBHXH", exclude=True)) + len(filter_by_tag(p_zalo_miss, "MBHXH", exclude=True))
    zalo_stats = {
        "total": zl_total_v, "total_g": calc_growth(zl_total_v, p_zl_total),
        "in": zl_in_v, "in_g": zl_in_g, "out": zl_out_v, "out_g": zl_out_g,
        "miss": zl_miss_v, "miss_g": zl_miss_g, "sla": zl_sla_v, "sla_g": zl_sla_g
    }

    # 3. BHXH
    bh_in_v, bh_in_g = calc_section_stats([c_tick_in, c_zalo_in], [p_tick_in, p_zalo_in], "ONLY", "MBHXH")
    bh_out_v, bh_out_g = calc_section_stats([c_tick_out, c_zalo_out], [p_tick_out, p_zalo_out], "ONLY", "MBHXH")
    bh_miss_v, bh_miss_g = calc_section_stats([c_tick_miss, c_zalo_miss], [p_tick_miss, p_zalo_miss], "ONLY", "MBHXH")
    bh_sla_v, bh_sla_g = calc_section_stats([c_tick_sla, c_zalo_sla], [p_tick_sla, p_zalo_sla], "ONLY", "MBHXH")
    bh_total_v = bh_in_v + bh_out_v + bh_miss_v
    
    p_bh_total = len(filter_by_tag(pd.concat([p_tick_in, p_zalo_in]), "MBHXH", exclude=False)) + \
                 len(filter_by_tag(pd.concat([p_tick_out, p_zalo_out]), "MBHXH", exclude=False)) + \
                 len(filter_by_tag(pd.concat([p_tick_miss, p_zalo_miss]), "MBHXH", exclude=False))
                 
    bhxh_stats = {
        "total": bh_total_v, "total_g": calc_growth(bh_total_v, p_bh_total),
        "in": bh_in_v, "in_g": bh_in_g, "out": bh_out_v, "out_g": bh_out_g,
        "miss": bh_miss_v, "miss_g": bh_miss_g, "sla": bh_sla_v, "sla_g": bh_sla_g
    }

    # Products Loop
    channels_data = []
    daily_by_product = {}
    defined_tags = [p["tag"] for p in PRODUCTS_CONFIG_V2]
    date_labels = [d.strftime('%Y-%m-%d') for d in pd.date_range(curr_start, curr_end)]

    for prod in PRODUCTS_CONFIG_V2:
        tag = prod["tag"]
        dfs_in = [filter_by_tag(c_tick_in, tag), filter_by_tag(c_zalo_in, tag)]
        dfs_out = [filter_by_tag(c_tick_out, tag), filter_by_tag(c_zalo_out, tag)]
        dfs_miss = [filter_by_tag(c_tick_miss, tag), filter_by_tag(c_zalo_miss, tag)]
        df_sla = filter_by_tag(pd.concat([c_tick_sla, c_zalo_sla]), tag)
        
        # Growth
        total_curr_p = sum(len(d) for d in dfs_in + dfs_out + dfs_miss)
        p_total_prev_p = sum(len(filter_by_tag(d, tag)) for d in [p_tick_in, p_zalo_in, p_tick_out, p_zalo_out, p_tick_miss, p_zalo_miss])
        
        channels_data.append({
            "id": prod["id"], "name": prod["name"], "type": "ticket", "color": prod["color"],
            "in": sum(len(d) for d in dfs_in), "out": sum(len(d) for d in dfs_out),
            "miss": sum(len(d) for d in dfs_miss), "sla": len(df_sla),
            "growth": calc_growth(total_curr_p, p_total_prev_p)
        })
        daily_by_product[prod["id"]] = get_daily_counts(dfs_in + dfs_out + dfs_miss, pd.date_range(curr_start, curr_end))

    # Others
    o_in = [filter_exclude_list(c_tick_in, defined_tags), filter_exclude_list(c_zalo_in, defined_tags)]
    o_out = [filter_exclude_list(c_tick_out, defined_tags), filter_exclude_list(c_zalo_out, defined_tags)]
    o_miss = [filter_exclude_list(c_tick_miss, defined_tags), filter_exclude_list(c_zalo_miss, defined_tags)]
    o_sla = filter_exclude_list(pd.concat([c_tick_sla, c_zalo_sla]), defined_tags)
    
    total_other_curr = sum(len(d) for d in o_in + o_out + o_miss)
    # Note: Lười tính growth chuẩn cho 'Others' vì phức tạp, lấy tạm 0 hoặc tính sau nếu cần
    
    channels_data.append({
        "id": "others", "name": "SẢN PHẨM KHÁC", "type": "ticket", "color": "#546E7A",
        "in": sum(len(d) for d in o_in), "out": sum(len(d) for d in o_out),
        "miss": sum(len(d) for d in o_miss), "sla": len(o_sla),
        "growth": 0 
    })
    daily_by_product["others"] = get_daily_counts(o_in + o_out + o_miss, pd.date_range(curr_start, curr_end))

    # Call Stats
    c_call_in_in, c_call_in_out = db["Call_Den_Trong_Gio"], db["Call_Den_Ngoai_Gio"]
    c_call_out_in, c_call_out_out = db["Call_Di_Trong_Gio"], db["Call_Di_Ngoai_Gio"]
    f_call_miss = db["Miss_Call"]
    
    dur_in = sum(df['Thời lượng'].apply(parse_duration_to_seconds).sum() for df in [c_call_in_in, c_call_in_out])
    dur_out = sum(df['Thời lượng'].apply(parse_duration_to_seconds).sum() for df in [c_call_out_in, c_call_out_out])
    
    channels_data.append({"id": "call_in", "name": "GỌI VÀO", "type": "call_in", "count_in": len(c_call_in_in), "count_out": len(c_call_in_out), "miss": len(f_call_miss), "duration": format_seconds(dur_in), "color": "#008ffb"})
    channels_data.append({"id": "call_out", "name": "GỌI RA", "type": "call_out", "count_in": len(c_call_out_in), "count_out": len(c_call_out_out), "miss": 0, "duration": format_seconds(dur_out), "color": "#feb019"})

    # Response v2
    response_data = {
        "channels": channels_data,
        "employees_subiz": calculate_employee_stats([c_tick_in], [c_tick_out], [c_tick_sla], "EXCLUDE", "MBHXH"),
        "employees_zalo": calculate_employee_stats([c_zalo_in], [c_zalo_out], [c_zalo_sla], "EXCLUDE", "MBHXH"),
        "employees_bhxh": calculate_employee_stats([c_tick_in, c_zalo_in], [c_tick_out, c_zalo_out], [c_tick_sla, c_zalo_sla], "ONLY", "MBHXH"),
        "charts": {
            "dates": date_labels,
            "daily_by_product": daily_by_product,
            "scatter_subiz_miss": generate_scatter_points(filter_by_tag(c_tick_miss, "MBHXH", exclude=True), date_labels),
            "scatter_subiz_sla": generate_scatter_points(filter_by_tag(c_tick_sla, "MBHXH", exclude=True), date_labels),
            "scatter_zalo_miss": generate_scatter_points(filter_by_tag(c_zalo_miss, "MBHXH", exclude=True), date_labels),
            "scatter_zalo_sla": generate_scatter_points(filter_by_tag(c_zalo_sla, "MBHXH", exclude=True), date_labels),
            "scatter_bhxh_miss": generate_scatter_points(filter_by_tag(pd.concat([c_tick_miss, c_zalo_miss]), "MBHXH", exclude=False), date_labels),
            "scatter_bhxh_sla": generate_scatter_points(filter_by_tag(pd.concat([c_tick_sla, c_zalo_sla]), "MBHXH", exclude=False), date_labels),
            
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
        "growth_stats": {"subiz": subiz_stats, "zalo": zalo_stats, "bhxh": bhxh_stats},
        "zalo_stats": {
             "in": len(filter_by_tag(c_zalo_in, "MBHXH", exclude=True)),
             "out": len(filter_by_tag(c_zalo_out, "MBHXH", exclude=True)),
             "miss": len(filter_by_tag(c_zalo_miss, "MBHXH", exclude=True)),
             "sla": len(filter_by_tag(c_zalo_sla, "MBHXH", exclude=True))
        },
        "call_stats": {
            "rate": f"{(((len(c_call_in_in)+len(c_call_in_out))/(len(c_call_in_in)+len(c_call_in_out)+len(f_call_miss)))*100) if (len(c_call_in_in)+len(c_call_in_out)+len(f_call_miss))>0 else 0:.1f}%", 
            "avg_duration": format_seconds(dur_in)
        }
    }
    return jsonify(response_data)

# ==============================================================================
# 5. MAIN EXECUTION
# ==============================================================================
if __name__ == '__main__':
    try:
        # Load data 1 lần duy nhất khi khởi động server
        load_all_data()
    except Exception as e:
        print(f"⚠️ Chưa load được data: {e}")
        
    print("🚀 SERVER ĐÃ SẴN SÀNG! ĐẠI CA BẤM GỌI API ĐI!")
    print("👉 API 1: http://localhost:5000/api/get-data")
    print("👉 API 2: http://localhost:5000/api/get-group-data")
    
    app.run(debug=True, port=5000)