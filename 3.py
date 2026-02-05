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
# 1. CẤU HÌNH HỆ THỐNG - KHÔNG CẦN DANH SÁCH NHÂN VIÊN
# ==============================================================================
# ID Folder Drive của Đại Ca
DRIVE_FOLDER_ID = "1056rTo3LQ9vGhjUAJMEZLUCG98DJedRC"

# Định nghĩa các loại dữ liệu cần cào (Tên file trên Drive)
DATA_CONFIG = {
    "Miss_Hoi_Thoai": "convo",  # Dùng logic cào hội thoại
    "Miss_Zalo":      "convo",  # Dùng logic cào hội thoại
    "Miss_Call":      "call"    # Dùng logic cào cuộc gọi
}

SCOPES = ['https://www.googleapis.com/auth/drive']

# ==============================================================================
# 2. HÀM QUẢN LÝ DRIVE (GIỮ NGUYÊN VÌ QUÁ NGON)
# ==============================================================================
def get_drive_service():
    """Mở cổng kết nối tới kho vàng (Drive)."""
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
    """Kéo file về check hàng."""
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
    """Đẩy hàng nóng lên mây."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    try:
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"      ✅ [Update] {file_name} ngon choét ({len(df)} dòng).")
        else:
            file_metadata = {'name': f"{file_name}.parquet", 'parents': [DRIVE_FOLDER_ID]}
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"      🆕 [New] {file_name} bóc tem thành công ({len(df)} dòng).")
    except Exception as e:
        print(f"      🔥 [LỖI] Toang khi upload {file_name}: {e}")

# ==============================================================================
# 3. BỘ ĐÔI SCRAPER: THỢ CÀO HỘI THOẠI & THỢ CÀO CALL
# ==============================================================================

# --- A. SCRAPER CHO HỘI THOẠI (Miss Hội Thoại, Miss Zalo) ---
def scrape_convo_data(driver, url, type_label):
    driver.get(url)
    time.sleep(8) # Chờ load hơi lâu tí cho chắc cốp
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
                    # Lấy tên khách
                    user = row.find_element(By.CSS_SELECTOR, "span.ml-3").text.strip()
                    if not user: continue
                    
                    # Lấy tags
                    tags = ", ".join([t.text.strip() for t in row.find_elements(By.CSS_SELECTOR, ".convo_tag__title")])
                    
                    # Lấy kênh (Channel) - Cột icon thường nằm ở td số 2 hoặc 3, check đại
                    try: 
                        cols = row.find_elements(By.TAG_NAME, "td")
                        channel_icon = cols[1].find_element(By.TAG_NAME, "i").get_attribute("class")
                    except: channel_icon = "N/A"

                    # Thời gian
                    created_time = row.find_element(By.XPATH, ".//td[last()]//span[@title]").get_attribute("title")
                    
                    item = {
                        "Loại": type_label,
                        "Khách hàng": user,
                        "Tags": tags,
                        "Channel_Code": channel_icon,
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
            time.sleep(3)
        except: break
    return scraped_data

# --- B. SCRAPER CHO CUỘC GỌI (Miss Call) ---
def scrape_call_missed(driver, url, type_label):
    driver.get(url)
    time.sleep(6)
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
                    if len(cols) < 5: continue 

                    # 1. SĐT
                    try: sdt = cols[1].text.strip()
                    except: sdt = "N/A"

                    # 2. Trạng thái (thường là Missed Call)
                    try: trang_thai = cols[2].text.strip()
                    except: trang_thai = "N/A"

                    # 3. Thời gian tạo
                    try:
                        time_span = cols[7].find_element(By.TAG_NAME, "span")
                        created_time = time_span.get_attribute("title")
                    except: 
                        # Fallback nếu cột lệch
                        try: created_time = row.find_element(By.XPATH, ".//td[last()]//span[@title]").get_attribute("title")
                        except: created_time = "N/A"

                    item = {
                        "SDT": sdt,
                        "Trạng thái": trang_thai,
                        "Thời gian tạo": created_time,
                        "Loại báo cáo": type_label
                    }
                    if item not in scraped_data: scraped_data.append(item)
                except: continue
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
# 4. LOGIC CHECK NGÀY MỚI (FAST & FURIOUS)
# ==============================================================================
def get_global_start_date(service):
    """
    Check 1 file đại diện (Miss_Hoi_Thoai), thấy ngày nào thì chốt luôn.
    """
    default_date = datetime.date(2026, 1, 1)
    print("\n🔍 Đệ đang check nhanh Drive...")

    # Check thử file đầu tiên trong list
    check_key = list(DATA_CONFIG.keys())[0] # "Miss_Hoi_Thoai"
    print(f"   👀 Ngó qua file: {check_key}...", end=" ")
    
    df, _ = download_file_by_name(service, check_key)
    
    if not df.empty and 'Ngay_Cào' in df.columns:
        try:
            dates = pd.to_datetime(df['Ngay_Cào'], format="%Y-%m-%d", errors='coerce').dt.date
            dates = dates.dropna()
            
            if not dates.empty:
                local_max = dates.max()
                print(f"✅ Thấy ngày Max: {local_max}")
                start_date = local_max + datetime.timedelta(days=1)
                print(f"🎯 => CHỐT HẠ: Cày tiếp từ ngày: {start_date}\n")
                return start_date
            else:
                print("❌ File có nhưng lỗi ngày.")
        except:
            print("❌ Lỗi format ngày.")
    else:
        print("❌ Chưa có file nào cả.")

    print(f"⚠️ => Drive sạch bong, bắt đầu cày từ đầu: {default_date}\n")
    return default_date

# ==============================================================================
# 5. MAIN PROGRAM
# ==============================================================================
def main():
    print("🚀 MISS REPORT SCRAPER - CHUYÊN TRỊ DATA BỊ SÓT")
    service = get_drive_service()
    
    # 1. Check ngày
    curr_date = get_global_start_date(service)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    
    if curr_date > yesterday:
        print("😎 Dữ liệu Miss đã update full rồi Đại Ca ơi!")
        return

    # 2. Bật Chrome
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    try:
        driver = webdriver.Chrome(options=options)
    except:
        print("🔥 Lỗi: Đại ca nhớ bật Chrome Debugger port 9222 nhé!")
        return

    # 3. Vòng lặp
    while curr_date <= yesterday:
        day_str = curr_date.strftime("%Y-%m-%d")
        print(f"📅 --- ĐANG CÀY DATA MISS NGÀY: {day_str} ---")
        
        # Tính toán ID thời gian Subiz (Công thức thần thánh)
        diff_days = (curr_date - datetime.date(2026, 1, 1)).days
        start_val = 490889 + (diff_days * 24)
        t_range = f"{start_val},{start_val + 23}"
        
        daily_storage = {k: [] for k in DATA_CONFIG.keys()}

        # --- XỬ LÝ TỪNG LOẠI URL ---
        
        # 1. MISS HỘI THOẠI
        print("   🔍 Quét Miss Hội Thoại...")
        url_hoi_thoai = f"https://app.subiz.com.vn/new-reports/convo-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22channel%22,%22value%22%3A%22%5B%5C%22email%5C%22,%5C%22subiz%5C%22,%5C%22facebook%5C%22,%5C%22facebook_comment%5C%22,%5C%22instagram%5C%22,%5C%22instagram_comment%5C%22,%5C%22form%5C%22,%5C%22google_review%5C%22%5D%22%7D,%7B%22key%22%3A%22agent_sent%22,%22value%22%3A%22%5B%5C%22no%5C%22%5D%22%7D,%7B%22key%22%3A%22tags%22,%22value%22%3A%22%5B%5C%22yes%5C%22,%5C%22tgrzpqjrknqhxliqelct%5C%22%5D%22%7D%5D"
        items_ht = scrape_convo_data(driver, url_hoi_thoai, "Miss_Hoi_Thoai")
        if items_ht: 
            for x in items_ht: x["Ngay_Cào"] = day_str
            daily_storage["Miss_Hoi_Thoai"].extend(items_ht)

        # 2. MISS ZALO
        print("   🔍 Quét Miss Zalo...")
        url_zalo = f"https://app.subiz.com.vn/new-reports/convo-list?conditions=%5B%7B%22key%22%3A%22created_time%22,%22value%22%3A%22%5B{t_range}%5D%22%7D,%7B%22key%22%3A%22channel%22,%22value%22%3A%22%5B%5C%22zalo_personal%5C%22,%5C%22zalo%5C%22%5D%22%7D,%7B%22key%22%3A%22agent_sent%22,%22value%22%3A%22%5B%5C%22no%5C%22%5D%22%7D,%7B%22key%22%3A%22tags%22,%22value%22%3A%22%5B%5C%22yes%5C%22,%5C%22tgrzpqjrknqhxliqelct%5C%22%5D%22%7D%5D"
        items_zl = scrape_convo_data(driver, url_zalo, "Miss_Zalo")
        if items_zl:
            for x in items_zl: x["Ngay_Cào"] = day_str
            daily_storage["Miss_Zalo"].extend(items_zl)

        # 3. MISS CALL
        print("   🔍 Quét Miss Call...")
        url_call = f"https://app.subiz.com.vn/new-reports/call-list?conditions=%5B%7B%22key%22%3A%22created_time%22%2C%22value%22%3A%22%5B{t_range}%5D%22%7D%2C%7B%22key%22%3A%22missed_call%22%7D%5D"
        items_call = scrape_call_missed(driver, url_call, "Miss_Call")
        if items_call:
            for x in items_call: x["Ngay_Cào"] = day_str
            daily_storage["Miss_Call"].extend(items_call)

        # --- SAVE TO DRIVE ---
        print(f"\n📦 [GOM HÀNG] Xong ngày {day_str}. Đẩy lên Drive...")
        for key in DATA_CONFIG.keys():
            if daily_storage[key]:
                df_old, f_id = download_file_by_name(service, key)
                new_df = pd.DataFrame(daily_storage[key])
                final_df = pd.concat([df_old, new_df], ignore_index=True)
                upload_to_drive(service, key, final_df, f_id)
            else:
                pass # Không có data thì im lặng là vàng

        curr_date += datetime.timedelta(days=1)

    print("\n💎 MISSION COMPLETED! Đại ca đẹp trai vô đối!")

if __name__ == "__main__":
    main()