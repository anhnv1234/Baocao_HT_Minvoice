import pandas as pd
import io
import os
import sys
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==============================================================================
# 1. CẤU HÌNH (ĐỪNG CHỈNH SỬA GÌ Ở ĐÂY NẾU KHÔNG MUỐN TOANG)
# ==============================================================================
DRIVE_FOLDER_ID = "1056rTo3LQ9vGhjUAJMEZLUCG98DJedRC"

# Danh sách 8 loại file đại ca yêu cầu
DATA_TYPES = [
    "Ticket_Trong_Gio", "Ticket_Ngoai_Gio", 
    "Zalo_Trong_Gio", "Zalo_Ngoai_Gio",
    "SLA_Ticket_Trong_Gio", "SLA_Ticket_Ngoai_Gio",
    "SLA_Zalo_Trong_Gio", "SLA_Zalo_Ngoai_Gio"
]
SCOPES = ['https://www.googleapis.com/auth/drive']

# Setup hiển thị Pandas full màn hình cho đại ca sướng mắt
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.float_format', '{:.0f}'.format) # Hiển thị số nguyên, bỏ .0

# ==============================================================================
# 2. HÀM KẾT NỐI & TẢI FILE (CHUẨN ISO CỦA ĐỆ)
# ==============================================================================
def get_drive_service():
    """Xin vé thông hành vào Drive"""
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

def download_file_content(service, file_name):
    """Móc lốp file từ Drive về RAM"""
    print(f"   ⏳ Đang kéo file: {file_name}...", end="\r")
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
            # Thêm cột loại để tí nữa phân biệt
            df['Loại_Dữ_Liệu'] = file_name 
            return df
        except:
            return pd.DataFrame()
    return pd.DataFrame()

# ==============================================================================
# 3. LOGIC XỬ LÝ SỐ LIỆU (PHẦN NÀY QUAN TRỌNG NHẤT)
# ==============================================================================
def main():
    print("\n" + "="*80)
    print("🕵️‍♂️  KÍNH CHIẾU YÊU V2 - SOI CHI TIẾT 8 HẠNG MỤC/NGÀY/NHÂN VIÊN")
    print("="*80)
    
    service = get_drive_service()
    all_data_frames = []

    # 1. Đi gom từng file một
    print("🚀 Bắt đầu quá trình thu thập dữ liệu...")
    for dtype in DATA_TYPES:
        df = download_file_content(service, dtype)
        if not df.empty:
            # Chỉ lấy các cột cần thiết để tính toán cho nhẹ
            if 'Ngay_Cào' in df.columns and 'Nhân viên hệ thống' in df.columns:
                # Group trước cho nhẹ RAM: Đếm số dòng theo Ngày & Nhân Viên
                grouped = df.groupby(['Ngay_Cào', 'Nhân viên hệ thống']).size().reset_index(name='So_Luong')
                grouped['Loại_Dữ_Liệu'] = dtype # Gán nhãn loại dữ liệu (ví dụ: Ticket_Trong_Gio)
                all_data_frames.append(grouped)
            else:
                print(f"   ⚠️  File {dtype} có tải về nhưng thiếu cột Ngày/Nhân viên. Bỏ qua!")
    
    print("\n   ✅ Đã tải xong toàn bộ dữ liệu! Đang xào nấu...")

    if not all_data_frames:
        print("❌ Toang! Không lấy được tí dữ liệu nào. Đại ca check lại Drive xem có file không?")
        return

    # 2. Gộp tất cả các bảng con lại thành 1 bảng to
    big_df = pd.concat(all_data_frames, ignore_index=True)

    # 3. Dùng tuyệt chiêu PIVOT TABLE để xoay bảng
    # Index (Dòng): Ngày, Nhân Viên
    # Columns (Cột): Các loại dữ liệu (8 loại)
    # Values (Giá trị): Số lượng
    final_report = big_df.pivot_table(
        index=['Ngay_Cào', 'Nhân viên hệ thống'], 
        columns='Loại_Dữ_Liệu', 
        values='So_Luong', 
        fill_value=0 # Nếu không có dữ liệu thì điền số 0
    )

    # Sắp xếp lại cột theo đúng thứ tự Đại Ca muốn (cho dễ nhìn)
    # Lọc ra những cột thực sự có trong dữ liệu (đề phòng trường hợp thiếu file)
    existing_cols = [col for col in DATA_TYPES if col in final_report.columns]
    final_report = final_report[existing_cols]

    # Sắp xếp dòng theo Ngày giảm dần (Mới nhất lên đầu) -> Rồi đến tên Nhân viên
    final_report = final_report.sort_index(level=[0, 1], ascending=[False, True])

    # 4. Xuất ra màn hình
    print("\n" + "="*100)
    print("📊 BẢNG TỔNG SẮP CHI TIẾT (Đơn vị: Số lượng record)")
    print("="*100)
    print(final_report)
    print("="*100)
    
    # Optional: Xuất ra CSV nếu đại ca muốn soi bằng Excel
    # final_report.to_csv("ket_qua_check_hang.csv")
    # print("💡 (Đã lưu thêm file 'ket_qua_check_hang.csv' cho đại ca dễ soi nếu bảng quá dài)")

    print("\n👉 Hướng dẫn đọc bảng:")
    print("   - Cột dọc bên trái: Ngày tháng và Tên nhân viên.")
    print("   - Các cột ngang: Số liệu thực tế đang lưu trên Drive.")
    print("   - Số 0: Có nghĩa là không tìm thấy bản ghi nào của loại đó (Có thể chưa cào hoặc không có).")
    print("💎 ĐẠI CA CHECK XEM KHỚP LỆNH CHƯA NHÉ!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Lỗi sấp mặt rồi đại ca ơi: {e}")