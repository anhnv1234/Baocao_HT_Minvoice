import pandas as pd
import io
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==============================================================================
# 1. CẤU HÌNH
# ==============================================================================
DRIVE_FOLDER_ID = "1056rTo3LQ9vGhjUAJMEZLUCG98DJedRC"
DATA_TYPES = [
    "Call_Den_Trong_Gio", 
    "Call_Di_Trong_Gio", 
    "Call_Den_Ngoai_Gio", 
    "Call_Di_Ngoai_Gio"
]
SCOPES = ['https://www.googleapis.com/auth/drive']

# Ngày cần kiểm tra
TARGET_DATE = "2026-01-01"

# Cấu hình hiển thị Pandas để không bị che khuất cột
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 30)

# ==============================================================================
# 2. HÀM KẾT NỐI DRIVE
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

def download_file_by_name(service, file_name):
    # Đã sửa lỗi dòng in ở đây
    print(f"⬇️ Đang tải file: {file_name}...", end="\r")
    
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
        
        # Đã sửa lỗi dòng in ở đây (thêm đóng ngoặc kép cẩn thận)
        print(f"✅ Đã tải xong: {file_name}      ")
        return pd.read_parquet(fh)
    
    print(f"❌ Không tìm thấy file: {file_name}")
    return pd.DataFrame()

# ==============================================================================
# 3. MAIN CHECK (IN 20 DÒNG)
# ==============================================================================
def main():
    print(f"🔍 --- KIỂM TRA DỮ LIỆU NGÀY {TARGET_DATE} ---")
    service = get_drive_service()
    
    for dtype in DATA_TYPES:
        print(f"\n{'='*60}")
        print(f"📂 LOẠI DỮ LIỆU: {dtype}")
        print(f"{'='*60}")
        
        df = download_file_by_name(service, dtype)
        
        if df.empty:
            print("   -> File rỗng hoặc không tồn tại trên Drive.")
            continue

        if 'Ngay_Cào' not in df.columns:
            print("   -> ⚠️ Lỗi: File này không có cột 'Ngay_Cào'.")
            continue

        # Lọc lấy dữ liệu ngày chỉ định
        df_target = df[df['Ngay_Cào'] == TARGET_DATE]
        count = len(df_target)

        print(f"📊 Tổng số dòng tìm thấy trong ngày {TARGET_DATE}: {count} dòng")

        if count > 0:
            print(f"\n👁️ --- HIỂN THỊ 20 DÒNG ĐẦU TIÊN ({dtype}) ---")
            # In ra 20 dòng đầu tiên của ngày hôm đó
            print(df_target.head(20).to_string(index=False))
            
            # Kiểm tra nhanh xem có dòng nào 0 phút không
            trash_df = df_target[
                df_target['Thời lượng'].astype(str).str.match(r'^(0\s|00:|0$)', case=False)
            ]
            if not trash_df.empty:
                 print(f"\n⚠️ CẢNH BÁO: Phát hiện {len(trash_df)} dòng có thời lượng = 0!")
        else:
            print(f"   -> Không có dữ liệu nào của ngày {TARGET_DATE} trong file này.")

    print("\n✅ Đã kiểm tra xong toàn bộ.")

if __name__ == "__main__":
    main()