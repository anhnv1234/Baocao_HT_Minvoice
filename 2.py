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
# 1. CẤU HÌNH HỆ THỐNG (ĐỒ CHƠI CỦA ĐẠI CA)
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
    "Call_Den_Trong_Gio", 
    "Call_Di_Trong_Gio", 
    "Call_Den_Ngoai_Gio", 
    "Call_Di_Ngoai_Gio"
]
SCOPES = ['https://www.googleapis.com/auth/drive']

# ==============================================================================
# 2. HÀM QUẢN LÝ DRIVE (KÉT SẮT CỦA ĐẠI CA)
# ==============================================================================
def get_drive_service():
    """Mở cửa kho Drive, check vé (token) đàng hoàng."""
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
    """Lôi cổ file trên Drive về để xem xét."""
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
        except Exception as e:
            print(f"      ☠️ Lỗi đọc file parquet {file_name}: {e}")
            return pd.DataFrame(), file_id
    return pd.DataFrame(), None

def upload_to_drive(service, file_name, df, file_id=None):
    """Đẩy hàng nóng lên mây."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    try:
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"      ✅ [Update] {file_name} ngon lành cành đào ({len(df)} dòng).")
        else:
            file_metadata = {'name': f"{file_name}.parquet", 'parents': [DRIVE_FOLDER_ID]}
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"      🆕 [New] {file_name} đập hộp thành công ({len(df)} dòng).")
    except Exception as e:
        print(f"      🔥 [LỖI] Không đẩy được file {file_name}: {e}")

# ==============================================================================
# 3. CORE SCRAPER - FORMAT CHUẨN & LỌC RÁC
# ==============================================================================
def scrape_call_data(driver, url, agent_name, type_label):
    driver.get(url)
    time.sleep(6) # Nghỉ tí cho mạng nó load
    
    scraped_data = []
    total_items = 0
    
    try:
        pagination_text_el = driver.find_element(By.XPATH, "//*[contains(text(), 'trong tổng số')]")
        match = re.search(r"tổng số\s+(\d+)", pagination_text_el.text)
        if match: total_items = int(match.group(1))
    except: pass

    if total_items == 0: return []

    while True:
        try:
            scroll = driver.find_element(By.CSS_SELECTOR, ".scroll-table-wrapper")
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll)
            time.sleep(1.5)
            
            rows = driver.find_elements(By.CSS_SELECTOR, "table.scroll-table tbody tr")
            if not rows: rows = driver.find_elements(By.CSS_SELECTOR, "table.scroll-table tr")
            
            for row in rows:
                try:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) < 8: continue 

                    # 4. XỬ LÝ THỜI LƯỢNG (VIP PRO MAX)
                    try:
                        raw_time = cols[4].get_attribute("textContent").strip()
                    except: raw_time = "0 phút"

                    # -- Format lại "50 giây" thành "0 phút 50 giây" --
                    if re.match(r'^\d+\s*giây$', raw_time):
                         thoi_luong = f"0 phút {raw_time}"
                    else:
                        thoi_luong = raw_time

                    # -- Bộ lọc thông minh --
                    has_real_value = False
                    for char in thoi_luong:
                        if char.isdigit() and char != '0':
                            has_real_value = True
                            break
                    
                    if not has_real_value: continue # Bỏ qua mấy cuộc 0s

                    # 1. SĐT
                    try: sdt = cols[1].text.strip()
                    except: sdt = "N/A"

                    # 2. Trạng thái
                    try: trang_thai = cols[2].text.strip()
                    except: trang_thai = "N/A"

                    # 3. Tags
                    try:
                        tag_elements = cols[3].find_elements(By.CSS_SELECTOR, ".convo_tag__title")
                        tags = ", ".join([t.text.strip() for t in tag_elements])
                    except: tags = ""

                    # 5. Agent
                    try:
                        agent_img = cols[6].find_element(By.TAG_NAME, "img")
                        agent_real = agent_img.get_attribute("title")
                    except: 
                        agent_real = cols[6].text.strip()

                    # 6. Thời gian tạo
                    try:
                        time_span = cols[7].find_element(By.TAG_NAME, "span")
                        created_time = time_span.get_attribute("title")
                    except: created_time = "N/A"

                    item = {
                        "SDT": sdt,
                        "Trạng thái": trang_thai,
                        "Tags": tags,
                        "Thời lượng": thoi_luong, # Đã format đẹp trai
                        "Agent thực hiện": agent_real,
                        "Thời gian tạo": created_time,
                        "Nhân viên hệ thống": agent_name,
                        "Loại cuộc gọi": type_label
                    }
                    
                    if item not in scraped_data: scraped_data.append(item)
                except Exception as e:
                    continue 

        except: break

        if len(scraped_data) >= total_items: break
        
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, ".lead-actions__paginate button:last-child")
            if next_btn.get_attribute("disabled"): break 
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(3)
        except: break
        
    return scraped_data

# ==============================================================================
# 4. LOGIC CHECK NGÀY MỚI - FAST & FURIOUS
# ==============================================================================
def get_global_start_date(service):
    """
    Check kiểu 'đánh nhanh rút gọn'.
    Chỉ cần 1 file có dữ liệu -> Lấy Max Date -> +1 ngày -> Chốt luôn (Return ngay).
    Không cần check 3 file còn lại.
    """
    default_date = datetime.date(2026, 1, 1)
    
    print("\n🔍 Đệ đang check nhanh Drive để chốt ngày cày...")

    for dtype in DATA_TYPES:
        print(f"   👀 Ngó qua file: {dtype}...", end=" ")
        df, _ = download_file_by_name(service, dtype)
        
        # Nếu file có dữ liệu và có cột Ngay_Cào
        if not df.empty and 'Ngay_Cào' in df.columns:
            try:
                dates = pd.to_datetime(df['Ngay_Cào'], format="%Y-%m-%d", errors='coerce').dt.date
                dates = dates.dropna()
                
                if not dates.empty:
                    local_max = dates.max()
                    print(f"✅ Thấy ngày {local_max}.")
                    
                    # LOGIC QUAN TRỌNG: Thấy phát là chốt luôn, không check tiếp
                    start_date = local_max + datetime.timedelta(days=1)
                    print(f"🎯 => CHỐT HẠ: Dữ liệu đã update đủ đến {local_max}. Bắt đầu cày từ: {start_date}\n")
                    return start_date
                else:
                    print("❌ File có nhưng lỗi ngày. Check tiếp file sau...")
            except Exception as e:
                print(f"❌ Lỗi format: {e}. Check tiếp file sau...")
        else:
            print("❌ Trống hoặc không có file. Check tiếp file sau...")

    # Nếu check hết 4 file mà vẫn không thấy gì
    print(f"⚠️ => Drive sạch bong, bắt đầu cày từ đầu: {default_date}\n")
    return default_date

# ==============================================================================
# 5. MAIN FUNCTION
# ==============================================================================
def main():
    print("📞 CALL SCRAPER ULTRA - LOGIC SIÊU TỐC ĐỘ")
    service = get_drive_service()
    
    # 1. Check ngày bắt đầu (gặp phát chốt luôn)
    curr_date = get_global_start_date(service)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    
    if curr_date > yesterday:
        print("😴 Đại Ca ơi, dữ liệu cập nhật đến hôm qua rồi. Ngủ thôi!")
        return

    # 2. Khởi động trình duyệt
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    try:
        driver = webdriver.Chrome(options=options)
    except:
        print("🔥 Lỗi: Đại ca bật Chrome Debugger chưa đấy?")
        return

    # 3. Vòng lặp cày cuốc
    while curr_date <= yesterday:
        day_str = curr_date.strftime("%Y-%m-%d")
        print(f"📅 --- BẮT ĐẦU CÀY NGÀY: {day_str} ---")
        
        # Tính time_range Subiz
        diff_days = (curr_date - datetime.date(2026, 1, 1)).days
        start_val = 490889 + (diff_days * 24)
        t_range = f"{start_val},{start_val + 23}"
        
        daily_storage = {dtype: [] for dtype in DATA_TYPES}

        # --- A. CÀO DỮ LIỆU ---
        for agent in AGENT_LIST:
            print(f"   👤 {agent['name']} đang làm việc...", end="\r")
            
            url_configs = {
                "Call_Den_Trong_Gio": f"https://app.subiz.com.vn/new-reports/call-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22agent%22,%22value%22%3A%22%5C%22{agent['id']}%5C%22%22%7D,%7B%22key%22%3A%22direction%22,%22value%22%3A%22%5C%22inbound%5C%22%22%7D,%7B%22key%22%3A%22business_hours%22,%22value%22%3A%22%5C%22true%5C%22%22%7D%5D",
                "Call_Di_Trong_Gio":  f"https://app.subiz.com.vn/new-reports/call-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22agent%22,%22value%22%3A%22%5C%22{agent['id']}%5C%22%22%7D,%7B%22key%22%3A%22direction%22,%22value%22%3A%22%5C%22outbound%5C%22%22%7D,%7B%22key%22%3A%22business_hours%22,%22value%22%3A%22%5C%22true%5C%22%22%7D%5D",
                "Call_Den_Ngoai_Gio": f"https://app.subiz.com.vn/new-reports/call-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22agent%22,%22value%22%3A%22%5C%22{agent['id']}%5C%22%22%7D,%7B%22key%22%3A%22direction%22,%22value%22%3A%22%5C%22inbound%5C%22%22%7D,%7B%22key%22%3A%22business_hours%22,%22value%22%3A%22%5C%22false%5C%22%22%7D%5D",
                "Call_Di_Ngoai_Gio":  f"https://app.subiz.com.vn/new-reports/call-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22agent%22,%22value%22%3A%22%5C%22{agent['id']}%5C%22%22%7D,%7B%22key%22%3A%22direction%22,%22value%22%3A%22%5C%22outbound%5C%22%22%7D,%7B%22key%22%3A%22business_hours%22,%22value%22%3A%22%5C%22false%5C%22%22%7D%5D"
            }

            for dtype in DATA_TYPES:
                link = url_configs[dtype]
                new_items = scrape_call_data(driver, link, agent['name'], dtype)
                if new_items:
                    for item in new_items: item["Ngay_Cào"] = day_str
                    daily_storage[dtype].extend(new_items)
        
        # --- B. UPLOAD DRIVE (LÀM MỘT LẦN CHO CẢ NGÀY) ---
        print(f"\n📦 [GOM HÀNG] Đã cào xong ngày {day_str}. Bắt đầu đẩy lên Drive...")
        
        for dtype in DATA_TYPES:
            if daily_storage[dtype]:
                df_old, f_id = download_file_by_name(service, dtype)
                new_df = pd.DataFrame(daily_storage[dtype])
                final_df = pd.concat([df_old, new_df], ignore_index=True)
                upload_to_drive(service, dtype, final_df, f_id)
            else:
                pass 

        curr_date += datetime.timedelta(days=1)
    
    print("\n💎 NHIỆM VỤ HOÀN THÀNH! Đại ca về nghỉ ngơi đi ạ!")

if __name__ == "__main__":
    main()