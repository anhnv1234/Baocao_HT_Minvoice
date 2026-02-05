import sys
import os
import threading
import time
import webbrowser
import importlib.util
import tkinter as tk
from tkinter import scrolledtext, ttk
from flask import Flask, send_from_directory
from flask_cors import CORS
from datetime import datetime

# ==============================================================================
# 1. CẤU HÌNH GIAO DIỆN (THEME DARK PRO)
# ==============================================================================
THEME = {
    "bg_main": "#1e1e1e",       # Nền chính
    "bg_log": "#252526",        # Nền log
    "fg_text": "#cccccc",       # Chữ thường
    "accent": "#007acc",        # Màu xanh VS Code (Thanh tiến trình)
    "success": "#4ec9b0",       # Xanh ngọc
    "warning": "#dcdcaa",       # Vàng nhạt
    "error": "#f44747",         # Đỏ cam
    "header": "#569cd6",        # Xanh dương
    "timestamp": "#6a9955",     # Xanh lá tối
    "file_tag": "#ce9178"       # Màu cam đất (Tên file)
}

# ==============================================================================
# 2. CLASS XỬ LÝ LOG & GUI (FIXED HIỂN THỊ)
# ==============================================================================
class ModernApp:
    def __init__(self, root):
        self.root = root
        self.root.title("SYSTEM MONITOR - ĐẠI CA TOOLS")
        self.root.geometry("950x650")
        self.root.configure(bg=THEME["bg_main"])
        
        # --- HEADER ---
        header_frame = tk.Frame(root, bg=THEME["bg_main"])
        header_frame.pack(fill="x", padx=20, pady=(15, 5))
        
        lbl_title = tk.Label(header_frame, text="🚀 LIVE MONITORING SYSTEM", 
                             font=("Segoe UI", 16, "bold"), fg="#ffffff", bg=THEME["bg_main"])
        lbl_title.pack(side="left")

        self.lbl_status = tk.Label(header_frame, text="Waiting for command...", 
                                   font=("Consolas", 10), fg="gray", bg=THEME["bg_main"])
        self.lbl_status.pack(side="right", anchor="s")

        # --- PROGRESS BAR ---
        progress_frame = tk.Frame(root, bg=THEME["bg_main"])
        progress_frame.pack(fill="x", padx=20, pady=5)
        
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("green.Horizontal.TProgressbar", 
                        foreground=THEME["accent"], background=THEME["accent"], 
                        troughcolor="#333333", bordercolor="#333333", 
                        lightcolor=THEME["accent"], darkcolor=THEME["accent"])
        
        self.progress = ttk.Progressbar(progress_frame, style="green.Horizontal.TProgressbar", 
                                        orient="horizontal", length=100, mode='determinate')
        self.progress.pack(fill="x")

        # --- LOG AREA ---
        log_frame = tk.Frame(root, bg=THEME["bg_main"], bd=1, relief="flat")
        log_frame.pack(expand=True, fill="both", padx=20, pady=10)

        self.log_widget = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=("Consolas", 10), 
                                                    bg=THEME["bg_log"], fg=THEME["fg_text"], 
                                                    insertbackground="white", borderwidth=0)
        self.log_widget.pack(expand=True, fill="both")

        # Tags màu
        self.log_widget.tag_config("TIME", foreground=THEME["timestamp"])
        self.log_widget.tag_config("INFO", foreground=THEME["fg_text"])
        self.log_widget.tag_config("SUCCESS", foreground=THEME["success"], font=("Consolas", 10, "bold"))
        self.log_widget.tag_config("WARNING", foreground=THEME["warning"])
        self.log_widget.tag_config("ERROR", foreground=THEME["error"], font=("Consolas", 10, "bold"))
        self.log_widget.tag_config("HEADER", foreground=THEME["header"], font=("Consolas", 11, "bold"))
        self.log_widget.tag_config("FILE", foreground=THEME["file_tag"], font=("Consolas", 10, "italic"))

        # Redirect Stdout
        sys.stdout = self
        sys.stderr = self

    def write(self, message):
        """Hàm bắt log chuẩn chỉ, không ép xuống dòng bừa bãi"""
        # 1. Fix lỗi bytes (quan trọng)
        if isinstance(message, bytes):
            try:
                message = message.decode('utf-8', errors='replace')
            except:
                message = str(message)
        
        if not message: return
        
        # 2. Xử lý logic hiển thị
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Đoán loại log để tô màu
        tag = "INFO"
        clean_msg = message.strip()
        if "Error" in clean_msg or "Lỗi" in clean_msg or "Exception" in clean_msg or "Crash" in clean_msg:
            tag = "ERROR"
        elif "Success" in clean_msg or "OK" in clean_msg or "Đã" in clean_msg or "Ready" in clean_msg or "Serving" in clean_msg:
            tag = "SUCCESS"
        elif "Down" in clean_msg or "Tải" in clean_msg or "Wait" in clean_msg or "Load" in clean_msg or "nạp" in clean_msg:
            tag = "WARNING"
        elif "BƯỚC" in clean_msg or "===" in clean_msg:
            tag = "HEADER"

        # Đẩy vào GUI (Thread safe)
        self.root.after(0, self._append_log, timestamp, message, tag)

    def _append_log(self, timestamp, message, tag):
        self.log_widget.configure(state='normal')
        
        # Chỉ in thời gian nếu message bắt đầu dòng mới (không bắt đầu bằng khoảng trắng)
        # Hoặc đơn giản là in thời gian nếu message có độ dài đáng kể
        if message.strip(): 
             # Kiểm tra xem dòng cuối cùng có phải là xuống dòng không
            last_char = self.log_widget.get("end-2c", "end-1c")
            if last_char == "\n" or last_char == "":
                self.log_widget.insert(tk.END, f"[{timestamp}] ", "TIME")

        start_idx = self.log_widget.index(tk.END)
        # QUAN TRỌNG: Không cộng thêm "\n" ở đây, để tôn trọng lệnh print(end=' ') của file gốc
        self.log_widget.insert(tk.END, message, tag)
        
        # Highlight tên file
        for fname in ["1_map.py", "2_map.py", "Dashboard_Live.html"]:
            search_start = "1.0"
            while True:
                pos = self.log_widget.search(fname, search_start, stopindex=tk.END)
                if not pos: break
                end_pos = f"{pos}+{len(fname)}c"
                self.log_widget.tag_add("FILE", pos, end_pos)
                search_start = end_pos

        self.log_widget.see(tk.END)
        self.log_widget.configure(state='disabled')

    def flush(self): pass

    def update_progress(self, value, text=None):
        self.root.after(0, lambda: self._update_prog_ui(value, text))

    def _update_prog_ui(self, value, text):
        self.progress['value'] = value
        if text:
            self.lbl_status.config(text=text)

# ==============================================================================
# 3. LOGIC SERVER
# ==============================================================================
def get_base_path():
    if getattr(sys, 'frozen', False): return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))

def load_and_run(app_gui):
    base_path = get_base_path()
    app_gui.update_progress(5, "Khởi động hệ thống...")
    
    print("\n==================================================")
    print("   🔧 BẮT ĐẦU QUY TRÌNH KẾT NỐI SERVER")
    print("==================================================")

    # --- LOAD MODULES ---
    app_gui.update_progress(10, "Đang đọc source code...")
    files = {"module_1": "1_map.py", "module_2": "2_map.py"}
    modules = {}

    for name, fname in files.items():
        fpath = os.path.join(base_path, fname)
        if not os.path.exists(fpath):
            print(f"❌ Lỗi: Không tìm thấy file {fname}")
            return
        
        try:
            print(f"📄 Đang nạp file: {fname}...")
            spec = importlib.util.spec_from_file_location(name, fpath)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            spec.loader.exec_module(mod)
            modules[name] = mod
            print(f"✅ Nạp thành công: {fname}")
            
            # Update progress bar
            curr = app_gui.progress['value']
            app_gui.update_progress(curr + 10, f"Đã nạp {fname}")
        except Exception as e:
            print(f"❌ Lỗi nạp {fname}: {e}")
            return

    # --- LOAD DATA ---
    print("\n⬇️  BƯỚC 2: TẢI DỮ LIỆU TỪ GOOGLE DRIVE...")
    app_gui.update_progress(40, "Đang tải dữ liệu (Vui lòng đợi)...")
    
    # Kích hoạt tải data
    data_loaded_count = 0
    for name, mod in modules.items():
        if hasattr(mod, "load_all_data"):
            try:
                print(f"   📦 Kích hoạt tải data cho {name}...")
                mod.load_all_data() # <--- GỌI HÀM NÀY NÓ MỚI IN RA LOG TẢI FILE
                data_loaded_count += 1
            except Exception as e:
                print(f"   ⚠️ Lỗi tải data {name}: {e}")
        else:
            print(f"   ℹ️ Module {name} không có hàm 'load_all_data'.")

    if data_loaded_count > 0:
        print("✅ Đã tải xong dữ liệu cần thiết.")
    
    app_gui.update_progress(70, "Cấu hình Server...")

    # --- MERGE SERVER ---
    print("\n🔗 BƯỚC 3: HỢP NHẤT API...")
    try:
        master_app = modules["module_1"].app
        slave_app = modules["module_2"].app
        CORS(master_app) 

        @master_app.route('/')
        def index():
            html_path = "Dashboard_Live.html"
            if os.path.exists(os.path.join(base_path, html_path)):
                return send_from_directory(base_path, html_path)
            return "<h1>⚠️ Không tìm thấy file Dashboard_Live.html</h1>"

        count = 0
        for rule in slave_app.url_map.iter_rules():
            if rule.endpoint != 'static':
                func = slave_app.view_functions[rule.endpoint]
                ep = f"mod2_{rule.endpoint}"
                try:
                    master_app.add_url_rule(rule.rule, endpoint=ep, view_func=func, methods=rule.methods)
                    count += 1
                except: pass
        print(f"✅ Đã ghép {count} luồng API vào hệ thống.")
        
    except Exception as e:
        print(f"❌ Lỗi merge server: {e}")
        return

    # --- LAUNCH ---
    app_gui.update_progress(90, "Đang mở trình duyệt...")
    print("\n🌍 BƯỚC 4: OPENING DASHBOARD...")
    
    html_file = os.path.join(base_path, "Dashboard_Live.html")
    if os.path.exists(html_file):
        webbrowser.open(f"file:///{os.path.abspath(html_file)}")
        print("✅ Đã mở tab Dashboard trên trình duyệt.")
    
    app_gui.update_progress(100, "SERVER ĐANG CHẠY (PORT 5000)")
    print("\n⚡ SERVER IS RUNNING AT PORT 5000...")
    print("👉 Đại ca cứ để cửa sổ này chạy nhé!")

    try:
        master_app.run(debug=False, port=5000, use_reloader=False)
    except Exception as e:
        print(f"❌ Server Crash: {e}")

# ==============================================================================
# 4. MAIN RUN
# ==============================================================================
def main():
    root = tk.Tk()
    app = ModernApp(root)
    t = threading.Thread(target=load_and_run, args=(app,))
    t.daemon = True
    root.after(1000, t.start)
    root.mainloop()

if __name__ == "__main__":
    main()