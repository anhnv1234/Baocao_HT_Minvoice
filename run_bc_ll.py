import importlib.util
import sys
import threading
from flask import Flask

# ==============================================================================
# HÀM TRIỆU HỒI ĐỆ TỬ (Load module từ file có tên bắt đầu bằng số)
# ==============================================================================
def import_module_from_file(file_path, module_name):
    """
    Hàm này dùng để import mấy file có tên kiểu '1_map.py' hay '2_map.py'
    bình thường Python nó không cho import số ở đầu, nên phải dùng tà thuật này.
    """
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            print(f"❌ Không tìm thấy file: {file_path}")
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        print(f"✅ Đã triệu hồi thành công: {file_path}")
        return module
    except Exception as e:
        print(f"💀 Lỗi khi load file {file_path}: {e}")
        return None

# ==============================================================================
# LOGIC HỢP THỂ (MERGE APPS)
# ==============================================================================
print("\n🔄 Đang tiến hành hợp nhất 2 luồng chân khí...")

# 1. Load 2 file gốc (Đại ca nhớ để 2 file này cùng thư mục với file launcher này nhé)
mod1 = import_module_from_file("1_map.py", "module_map_1")
mod2 = import_module_from_file("2_map.py", "module_map_2")

if not mod1 or not mod2:
    print("❌ Thất bại! Kiểm tra lại xem 2 file 1_map.py và 2_map.py có đúng tên chưa đại ca?")
    sys.exit(1)

# 2. Lấy Flask app từ module 1 làm "Vật Chủ" (Host chính)
master_app = mod1.app 

# 3. Móc toàn bộ các đường dẫn (Routes) của App 2 gắn sang App 1
# Kỹ thuật này gọi là "Route Grafting" - Ghép cành
# Vì file 1 dùng /api/get-data còn file 2 dùng /api/get-group-data nên không sợ đụng hàng.
print("🔗 Đang map API từ file 2 sang file 1...")

# Duyệt qua tất cả các rules (đường dẫn) của app 2
for rule in mod2.app.url_map.iter_rules():
    if rule.endpoint != 'static': # Bỏ qua folder static mặc định
        # Lấy view function (hàm xử lý) tương ứng từ app 2
        view_func = mod2.app.view_functions[rule.endpoint]
        
        # Gắn hàm đó vào master_app với cùng đường dẫn và phương thức (GET/POST)
        # Lưu ý: Chúng ta dùng context của mod2, nên các biến toàn cục trong file 2 vẫn chạy ngon.
        options = {
            "methods": rule.methods,
            "defaults": rule.defaults
            # "endpoint": rule.endpoint # Flask tự xử lý endpoint unique nếu cần
        }
        
        # Tránh lỗi trùng endpoint (tên hàm) nếu 2 file đặt tên hàm giống nhau
        endpoint_name = f"mod2_{rule.endpoint}" 
        
        try:
            master_app.add_url_rule(
                rule.rule, 
                endpoint=endpoint_name, 
                view_func=view_func, 
                **options
            )
            print(f"   ➕ Đã ghép: {rule.rule} -> chạy logic của file 2")
        except AssertionError:
            print(f"   ⚠️ Trùng lặp route: {rule.rule} (Đã có ở file 1, bỏ qua)")

# ==============================================================================
# KHỞI ĐỘNG SERVER
# ==============================================================================
if __name__ == '__main__':
    print("\n🚀 HỆ THỐNG ĐÃ SẴN SÀNG! ĐẠI CA LÊN NHẠC!")
    print(f"👉 API File 1: http://localhost:5000/api/get-data")
    print(f"👉 API File 2: http://localhost:5000/api/get-group-data")
    
    # Chạy master_app (đã bao gồm cả nội công của app 2)
    # Tắt debug reloader để tránh lỗi import lại 2 lần gây xung đột
    master_app.run(debug=True, port=5000, use_reloader=False)