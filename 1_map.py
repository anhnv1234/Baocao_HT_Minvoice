from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import io
import os
import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==============================================================================
# 1. CẤU HÌNH (GIỮ NGUYÊN)
# ==============================================================================
app = Flask(__name__)
CORS(app) 

DRIVE_FOLDER_ID = "1056rTo3LQ9vGhjUAJMEZLUCG98DJedRC"
SCOPES = ['https://www.googleapis.com/auth/drive']

MASTER_PRODUCTS = [
    "Subiz 1.0", "Subiz 2.0", "SMI (Hóa Đơn)", "Hỗ trợ BHXH", 
    "MCTTNCN (Thuế)", "Hỗ trợ CKS", "M2SALE", "MSELLER (Máy POS)",
    "Zalo OA", "Tổng đài GỌI VÀO", "Tổng đài GỌI RA", "Sản phẩm khác"
]

INTERACTION_FILES = [
    "Ticket_Trong_Gio", "Ticket_Ngoai_Gio", "Zalo_Trong_Gio", "Zalo_Ngoai_Gio",
    "Call_Den_Trong_Gio", "Call_Den_Ngoai_Gio", "Call_Di_Trong_Gio", "Call_Di_Ngoai_Gio"
]

SLA_FILES = [
    "SLA_Ticket_Trong_Gio", "SLA_Ticket_Ngoai_Gio", "SLA_Zalo_Trong_Gio", "SLA_Zalo_Ngoai_Gio"
]

FILES_TO_LOAD = INTERACTION_FILES + SLA_FILES

# ==============================================================================
# 2. HÀM KẾT NỐI & TẢI FILE
# ==============================================================================
def get_drive_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token: creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token: token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def download_df(service, filename):
    try:
        query = f"name = '{filename}.parquet' and '{DRIVE_FOLDER_ID}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        if not files: return pd.DataFrame()
        request = service.files().get_media(fileId=files[0]['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        return pd.read_parquet(fh)
    except: return pd.DataFrame()

# ==============================================================================
# 3. LOGIC XỬ LÝ DỮ LIỆU
# ==============================================================================
def detect_product_v3(row, src):
    tags = str(row.get('Tags', '')).upper()
    if "Call_Den" in src: return "Tổng đài GỌI VÀO"
    if "Call_Di" in src: return "Tổng đài GỌI RA"
    if "MBHXH" in tags: return "Hỗ trợ BHXH"
    if "Ticket" in src:
        if "EINVOICE1.0" in tags: return "Subiz 1.0"
        if "EINVOICE2.0" in tags or "EINVOICE2.0" in tags: return "Subiz 2.0"
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

def calculate_growth(current_val, prev_val):
    if prev_val == 0:
        return 100 if current_val > 0 else 0
    return round(((current_val - prev_val) / prev_val) * 100, 1)

# ==============================================================================
# 4. API ENDPOINT
# ==============================================================================
@app.route('/api/get-data', methods=['GET'])
def get_dashboard_data():
    date_start_str = request.args.get('start')
    date_end_str = request.args.get('end')
    
    # 1. Tải tất cả file từ Drive
    service = get_drive_service()
    all_data = []
    
    for fname in FILES_TO_LOAD:
        df = download_df(service, fname)
        if not df.empty:
            df['Source_File'] = fname
            df['Is_Interaction'] = 1 if fname in INTERACTION_FILES else 0
            df['Is_SLA_File'] = 1 if fname in SLA_FILES else 0
            time_col = 'Thời gian' if 'Thời gian' in df.columns else 'Thời gian tạo'
            df['Date_Obj'] = pd.to_datetime(df[time_col], errors='coerce')
            all_data.append(df)
            
    if not all_data: return jsonify({}) 

    full_df = pd.concat(all_data, ignore_index=True)
    full_df['Product_Label'] = full_df.apply(lambda x: detect_product_v3(x, x['Source_File']), axis=1)
    full_df['Minutes'] = full_df.apply(lambda x: parse_minutes(x, x['Source_File']), axis=1)

    # 2. Xác định khoảng thời gian lọc & khoảng so sánh (để tính Growth)
    d_start = pd.to_datetime(date_start_str).date()
    d_end = pd.to_datetime(date_end_str).date()
    
    # Tính delta ngày để tìm kỳ trước (Previous Period)
    delta_days = (d_end - d_start).days + 1
    d_prev_end = d_start - datetime.timedelta(days=1)
    d_prev_start = d_prev_end - datetime.timedelta(days=delta_days - 1)

    # 3. Chia dữ liệu thành 2 tập: Hiện tại và Quá khứ
    df_curr = full_df[(full_df['Date_Obj'].dt.date >= d_start) & (full_df['Date_Obj'].dt.date <= d_end)]
    df_prev = full_df[(full_df['Date_Obj'].dt.date >= d_prev_start) & (full_df['Date_Obj'].dt.date <= d_prev_end)]

    total_room_interaction = len(df_curr[df_curr['Is_Interaction'] == 1])

    output_db = {}
    agents = [a for a in full_df['Nhân viên hệ thống'].unique() if a]
    
    for agent in agents:
        # --- Lấy dữ liệu của Agent ---
        u_curr = df_curr[df_curr['Nhân viên hệ thống'] == agent]
        u_prev = df_prev[df_prev['Nhân viên hệ thống'] == agent]

        if u_curr.empty and u_prev.empty:
            continue

        # --- Tổng hợp số liệu Tổng quát ---
        u_curr_inter = u_curr[u_curr['Is_Interaction'] == 1]
        u_prev_inter = u_prev[u_prev['Is_Interaction'] == 1]
        
        # Trong giờ / Ngoài giờ
        t_in = len(u_curr_inter[u_curr_inter['Source_File'].str.contains('Trong_Gio')])
        t_out = len(u_curr_inter[u_curr_inter['Source_File'].str.contains('Ngoai_Gio')])
        total_curr = t_in + t_out
        total_prev = len(u_prev_inter)

        # SLA stats
        u_curr_sla = u_curr[u_curr['Is_SLA_File'] == 1]
        u_prev_sla = u_prev[u_prev['Is_SLA_File'] == 1]
        
        sla_count_curr = len(u_curr_sla)
        sla_count_prev = len(u_prev_sla)

        # Tính toán Growth Tổng
        total_growth = calculate_growth(total_curr, total_prev)
        sla_growth = calculate_growth(sla_count_curr, sla_count_prev)

        # --- Tổng hợp từng Metric (Sản phẩm) ---
        metrics = []
        for p in MASTER_PRODUCTS:
            # Lọc theo sản phẩm ở kỳ hiện tại
            p_curr = u_curr[u_curr['Product_Label'] == p]
            p_curr_inter = p_curr[p_curr['Is_Interaction'] == 1]
            
            # Lọc theo sản phẩm ở kỳ trước
            p_prev = u_prev[u_prev['Product_Label'] == p]
            p_prev_inter = p_prev[p_prev['Is_Interaction'] == 1]

            # Số liệu chi tiết
            p_in = len(p_curr_inter[p_curr_inter['Source_File'].str.contains('Trong_Gio')])
            p_out = len(p_curr_inter[p_curr_inter['Source_File'].str.contains('Ngoai_Gio')])
            p_total_curr = p_in + p_out
            p_total_prev = len(p_prev_inter)
            
            p_sla_count = len(p_curr[p_curr['Is_SLA_File'] == 1])
            min_val = round(p_curr['Minutes'].sum(), 2) if "Tổng đài" in p else None
            
            # Tính Growth từng sản phẩm
            p_growth = calculate_growth(p_total_curr, p_total_prev)
            
            metrics.append({
                "id": p, "name": p, "in": p_in, "out": p_out, "sla": p_sla_count,
                "growth": p_growth, "minutes": min_val
            })

        # --- Logic Hourly Series (Dữ liệu biểu đồ giờ) ---
        hourly_series = []
        if not u_curr_inter.empty:
            for p in MASTER_PRODUCTS:
                p_inter = u_curr_inter[u_curr_inter['Product_Label'] == p]
                if not p_inter.empty:
                    # Group theo giờ (0-23)
                    counts = p_inter.groupby(p_inter['Date_Obj'].dt.hour).size().reindex(range(24), fill_value=0).tolist()
                    if sum(counts) > 0:
                        hourly_series.append({"label": p, "data": counts})

        # --- Logic Scatter Data (Mật độ lỗi SLA) ---
        scatter_sla = []
        if not u_curr_sla.empty:
            for _, row in u_curr_sla.iterrows():
                if pd.notnull(row['Date_Obj']):
                    scatter_sla.append({
                        "x": row['Date_Obj'].isoformat(),
                        "y": round(row['Date_Obj'].hour + row['Date_Obj'].minute/60, 2),
                        "label": row['Product_Label']
                    })

        # Đóng gói dữ liệu trả về
        output_db[agent] = {
            "name": agent,
            "avatar": agent[:2].upper(),
            "workDays": f"{u_curr['Date_Obj'].dt.date.nunique()} ngày",
            "totalIn": t_in, "totalOut": t_out, "total": total_curr,
            "totalRoom": total_room_interaction,
            "growth": total_growth, # Đã có giá trị thật
            "stats": {
                "slaCount": sla_count_curr, 
                "slaGrowth": sla_growth # Đã có giá trị thật
            },
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

if __name__ == '__main__':
    app.run(debug=True, port=5000)