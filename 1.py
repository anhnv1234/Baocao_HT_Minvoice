import time
import datetime
import pandas as pd
import re
import io
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG CỦA ĐẠI CA (GIỮ NGUYÊN DANH SÁCH)
# ==============================================================================
AGENT_LIST = [
    {"name": "Nguyễn Việt Anh",   "id": "agrzhrqfkqrxgjegkm"},
    {"name": "Dương Đức Mạnh",    "id": "agsgkmrdysrrpzagne"},
    {"name": "Trần Văn Đạt",      "id": "agrzpfsnfimtdpxjpb"},
    {"name": "Tạ Hồng Vân",       "id": "agskdgddsbjsidnziq"},
    {"name": "Nguyễn Xuân Hưng",  "id": "agsjchhwrrmypatdya"},
    {"name": "Nguyễn Huy Hiệp",   "id": "agsgwwrcuxtprsuluv"},
    {"name": "Trịnh Hoài Nhất",   "id": "agrzherrzmqywsqtcj"},
    {"name": "Trần Hải Hưng",     "id": "agryguzgwxmadbaeif"},
    {"name": "Hà Trường Long",    "id": "agsmjjrywqbazheszg"},
    {"name": "Nguyễn Lam Trường", "id": "agrzhettmdrgyhkkqd"},
    {"name": "Lại Văn Võ",        "id": "agsjnxvcazufvduebg"},
    {"name": "Phạm Văn Tuân",     "id": "agrzhesbwfophgvxsy"},
    {"name": "Nguyễn Minh Đức",   "id": "agslmtmfwjfwroeotw"},
    {"name": "Nguyễn Thanh Tùng", "id": "agrznezyiwydqsqbjo"},
]

DRIVE_FOLDER_ID = "1056rTo3LQ9vGhjUAJMEZLUCG98DJedRC"
DATA_TYPES = [
    "Ticket_Trong_Gio", "Ticket_Ngoai_Gio", 
    "Zalo_Trong_Gio", "Zalo_Ngoai_Gio",
    "SLA_Ticket_Trong_Gio", "SLA_Ticket_Ngoai_Gio",
    "SLA_Zalo_Trong_Gio", "SLA_Zalo_Ngoai_Gio"
]
SCOPES = ['https://www.googleapis.com/auth/drive']

# ==============================================================================
# 2. HÀM QUẢN LÝ DRIVE (SOTA)
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
    """Tải file từ Drive, trả về DataFrame và file_id"""
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
            return pd.read_parquet(fh), file_id
        except:
            return pd.DataFrame(), file_id
    return pd.DataFrame(), None

def upload_to_drive(service, file_name, df, file_id=None):
    """Đẩy dữ liệu lên Drive (Cập nhật hoặc Tạo mới)"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"      ✅ Đã đẩy bản cập nhật: {file_name} ({len(df)} dòng)")
    else:
        file_metadata = {'name': f"{file_name}.parquet", 'parents': [DRIVE_FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"      🆕 Đã tạo mới file: {file_name} ({len(df)} dòng)")

# ==============================================================================
# 3. CORE SCRAPER (GIỮ NGUYÊN LOGIC LÌ LỢM)
# ==============================================================================
def scrape_data_classic(driver, url, agent_name, type_label):
    driver.get(url)
    time.sleep(10) 
    scraped_data = []
    total_items = 0
    
    try:
        pagination_text_el = driver.find_element(By.XPATH, "//*[contains(text(), 'trong tổng số')]")
        match = re.search(r"tổng số\s+(\d+)", pagination_text_el.text)
        if match: total_items = int(match.group(1))
    except:
        try:
            total_items = int(driver.find_element(By.CSS_SELECTOR, ".lead-actions__paginate-info b:last-child").text)
        except: pass

    if total_items == 0: return []

    while True:
        try:
            scroll = driver.find_element(By.CSS_SELECTOR, ".scroll-table-wrapper")
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll)
            time.sleep(1)
            
            rows = driver.find_elements(By.CSS_SELECTOR, "table.scroll-table tbody tr")
            if not rows: rows = driver.find_elements(By.CSS_SELECTOR, "table.scroll-table tr")
            
            for row in rows:
                try:
                    user = row.find_element(By.CSS_SELECTOR, "span.ml-3").text.strip()
                    if not user: continue
                    tags = ", ".join([t.text.strip() for t in row.find_elements(By.CSS_SELECTOR, ".convo_tag__title")])
                    
                    agent_subiz = "N/A"
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) > 6:
                        try: agent_subiz = cols[6].find_element(By.TAG_NAME, "img").get_attribute("title")
                        except: agent_subiz = cols[6].text.strip()

                    created_time = row.find_element(By.XPATH, ".//td[last()]//span[@title]").get_attribute("title")
                    
                    item = {
                        "Nhân viên hệ thống": agent_name,
                        "Loại": type_label,
                        "Khách hàng": user,
                        "Tags": tags,
                        "Agent Subiz": agent_subiz,
                        "Thời gian": created_time
                    }
                    if item not in scraped_data: scraped_data.append(item)
                except: continue
        except: break

        if len(scraped_data) >= total_items: break
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, ".lead-actions__paginate button:last-child")
            if next_btn.get_attribute("disabled"): break 
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(4)
        except: break
    return scraped_data

# ==============================================================================
# 4. LOGIC CHECK NGÀY MỚI (FAST & FURIOUS)
# ==============================================================================
def get_global_start_date(service):
    """
    Check siêu tốc: Chỉ cần 1 file trong 8 file có ngày X
    -> Coi như ngày X đã xong -> Trả về X + 1.
    Không check dai dòng.
    """
    default_date = datetime.date(2026, 1, 1)
    
    print("\n🔍 Đang soi Drive để tìm ngày cào tiếp theo...")

    for dtype in DATA_TYPES:
        print(f"   👀 Check nhanh file: {dtype}...", end=" ")
        df, _ = download_file_by_name(service, dtype)
        
        if not df.empty and 'Ngay_Cào' in df.columns:
            try:
                dates = pd.to_datetime(df['Ngay_Cào'], format="%Y-%m-%d", errors='coerce').dt.date
                dates = dates.dropna()
                
                if not dates.empty:
                    local_max = dates.max()
                    print(f"✅ Thấy ngày Max: {local_max}")
                    
                    # LOGIC ĂN TIỀN: Thấy phát chốt luôn, không check tiếp file khác
                    start_date = local_max + datetime.timedelta(days=1)
                    print(f"🎯 => CHỐT HẠ: Đã xong ngày {local_max}. Bắt đầu cày từ: {start_date}\n")
                    return start_date
                else:
                    print("❌ File có nhưng lỗi ngày.")
            except:
                print("❌ Lỗi format ngày.")
        else:
            print("❌ Trống hoặc không có file.")

    print(f"⚠️ => Drive sạch bong, bắt đầu cày từ đầu: {default_date}\n")
    return default_date

# ==============================================================================
# 5. RUNNER CHÍNH
# ==============================================================================
def main():
    print("🚀 TICKET SCRAPER VIP - PHIÊN BẢN ĐỆ TỬ RUỘT")
    service = get_drive_service()
    
    # 1. Check ngày bắt đầu (Fast Check)
    curr_date = get_global_start_date(service)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    
    if curr_date > yesterday:
        print("😎 Dữ liệu đã mới nhất rồi đại ca ơi! Nghỉ ngơi tán gái thôi.")
        return

    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    try:
        driver = webdriver.Chrome(options=options)
    except:
        print("🔥 Lỗi: Đại ca bật Chrome Debugger chưa đấy?")
        return

    while curr_date <= yesterday:
        day_str = curr_date.strftime("%Y-%m-%d")
        print(f"\n📅 --- ĐANG CÀY NGÀY: {day_str} ---")
        
        # Mapping ngày Subiz
        diff_days = (curr_date - datetime.date(2026, 1, 1)).days
        start_val = 490889 + (diff_days * 24)
        t_range = f"{start_val},{start_val + 23}"
        
        # Kho chứa dữ liệu gom cho cả ngày
        daily_storage = {dtype: [] for dtype in DATA_TYPES}

        # --- A. CÀO DỮ LIỆU ---
        for agent in AGENT_LIST:
            print(f"   👤 Đang quét: {agent['name']}")
            for dtype in DATA_TYPES:
                # Cấu hình link phức tạp của Đại Ca
                ch_t = "%5B%5C%22subiz%5C%22,%5C%22facebook%5C%22,%5C%22facebook_comment%5C%22,%5C%22instagram%5C%22,%5C%22instagram_comment%5C%22,%5C%22form%5C%22,%5C%22google_review%5C%22%5D"
                ch_z = "%5B%5C%22zalo_personal%5C%22,%5C%22zalo%5C%22%5D"
                ch_sla_t = "%5B%5C%22form%5C%22,%5C%22instagram_comment%5C%22,%5C%22instagram%5C%22,%5C%22email%5C%22,%5C%22facebook_comment%5C%22,%5C%22facebook%5C%22,%5C%22subiz%5C%22%5D"
                
                cfg = {
                    "Ticket_Trong_Gio": {"h": "true", "k": "as", "c": ch_t},
                    "Ticket_Ngoai_Gio": {"h": "false", "k": "as", "c": ch_t},
                    "Zalo_Trong_Gio": {"h": "true", "k": "as", "c": ch_z},
                    "Zalo_Ngoai_Gio": {"h": "false", "k": "as", "c": ch_z},
                    "SLA_Ticket_Trong_Gio": {"h": "true", "k": "sla", "c": ch_sla_t},
                    "SLA_Ticket_Ngoai_Gio": {"h": "false", "k": "sla", "c": ch_sla_t},
                    "SLA_Zalo_Trong_Gio": {"h": "true", "k": "sla", "c": ch_z},
                    "SLA_Zalo_Ngoai_Gio": {"h": "false", "k": "sla", "c": ch_z},
                }[dtype]

                if cfg["k"] == "as":
                    link = f"https://app.subiz.com.vn/new-reports/convo-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22channel%22,%22value%22%3A%22{cfg['c']}%22%7D,%7B%22key%22%3A%22agent_sent%22,%22value%22%3A%22%5B%5C%22yes%5C%22,%5C%22{agent['id']}%5C%22%5D%22%7D,%7B%22key%22%3A%22business_hours%22,%22value%22%3A%22%5C%22{cfg['h']}%5C%22%22%7D%5D"
                else:
                    link = f"https://app.subiz.com.vn/new-reports/convo-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22business_hours%22,%22value%22%3A%22%5C%22{cfg['h']}%5C%22%22%7D,%7B%22key%22%3A%22replied_duration%22,%22replied_duration_gt%22%3A%22600000%22,%22replied_duration_lte%22%3A%2286400000%22,%22type%22%3A%22gt%22%7D,%7B%22key%22%3A%22first_replied_duration_of%22,%22first_replied_duration_of%22%3A%22%5C%22{agent['id']}%5C%22%22%7D,%7B%22key%22%3A%22channel%22,%22value%22%3A%22{cfg['c']}%22%7D%5D"

                new_items = scrape_data_classic(driver, link, agent['name'], dtype)
                if new_items:
                    for item in new_items: item["Ngay_Cào"] = day_str
                    daily_storage[dtype].extend(new_items)
        
        # --- B. UPLOAD DRIVE (GOM CẢ NGÀY MỚI ĐẨY) ---
        print(f"📦 [GOM HÀNG] Đã xong ngày {day_str}. Bắt đầu đẩy lên Drive...")
        for dtype in DATA_TYPES:
            if daily_storage[dtype]:
                df_old, f_id = download_file_by_name(service, dtype)
                new_df = pd.DataFrame(daily_storage[dtype])
                final_df = pd.concat([df_old, new_df], ignore_index=True)
                upload_to_drive(service, dtype, final_df, f_id)
            else:
                # Không có dữ liệu thì thôi, không spam
                pass

        curr_date += datetime.timedelta(days=1)
    
    print("\n💎 HẾT NƯỚC CHẤM! Đã cập nhật xong xuôi tất cả!")

if __name__ == "__main__":
    main()