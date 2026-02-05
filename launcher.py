import subprocess
import sys
import time

# ==============================================================================
# MASTER RUNNER - ĐỆ TỬ TỔNG QUẢN CỦA ĐẠI CA ĐẸP TRAI
# ==============================================================================

def run_script(script_name):
    """Gọi hồn các file script của đại ca ra làm việc."""
    print(f"\n{'='*60}")
    print(f"🚀 ĐANG TRIÊU HỒI: {script_name}")
    print(f"{'='*60}")
    
    try:
        # Chạy file bằng chính trình thông dịch Python đang dùng
        # Chạy lần lượt (sequence), thằng này xong mới đến thằng kia
        process = subprocess.Popen(
            [sys.executable, script_name],
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True
        )
        
        # Chờ thằng đệ làm xong việc mới cho thằng tiếp theo vào
        process.wait()
        
        if process.returncode == 0:
            print(f"\n✅ {script_name} ĐÃ HOÀN THÀNH NHIỆM VỤ!")
        else:
            print(f"\n🔥 CẢNH BÁO: {script_name} CÓ BIẾN (Mã lỗi: {process.returncode})")
            
    except Exception as e:
        print(f"❌ Toang rồi đại ca ơi! Không chạy được {script_name}: {e}")

def main():
    start_time = time.time()
    
    print("💎 CHÀO ĐẠI CA ĐẸP TRAI! HỆ THỐNG BẮT ĐẦU CÀY DATA...")
    
    # Danh sách các file đệ tử cần gọi (Đúng tên file của đại ca)
    scripts = ["1.py", "2.py", "3.py"]
    
    for script in scripts:
        run_script(script)
        # Nghỉ tay 2 giây giữa các script cho trình duyệt kịp thở
        time.sleep(2)
    
    total_time = (time.time() - start_time) / 60
    print(f"\n{'*'*60}")
    print(f"💎 TẤT CẢ ĐÃ XONG XUÔI! TỔNG THỜI GIAN CÀY CUỐC: {total_time:.2f} PHÚT")
    print(f"ĐẠI CA ĐI TÁN GÁI TIẾP ĐI, MỌI THỨ ĐÃ LÊN CLOUD NGON CHOÉT!")
    print(f"{'*'*60}")

if __name__ == "__main__":
    main()