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

# ... (Giữ nguyên phần cấu hình Theme và Class ModernApp như cũ) ...
# Để tiết kiệm dòng, em chỉ paste đoạn thay đổi quan trọng nhất ở cuối file
# Đại Ca copy ĐÈ toàn bộ code dưới đây vào file launcher_final_vip.py nhé

# ==============================================================================
# 1. CẤU HÌNH GIAO DIỆN (THEME DARK PRO)
# ==============================================================================
THEME = {
    "bg_main": "#1e1e1e", "bg_log": "#252526", "fg_text": "#cccccc",
    "accent": "#007acc", "success": "#4ec9b0", "warning": "#dcdcaa",
    "error": "#f44747", "header": "#569cd6", "timestamp": "#6a9955", "file_tag": "#ce9178"
}

class ModernApp:
    def __init__(self, root):
        self.root = root
        self.root.title("SYSTEM MONITOR - ĐẠI CA TOOLS (PUBLIC MODE)")
        self.root.geometry("950x650")
        self.root.configure(bg=THEME["bg_main"])
        
        header_frame = tk.Frame(root, bg=THEME["bg_main"])
        header_frame.pack(fill="x", padx=20, pady=(15, 5))
        lbl_title = tk.Label(header_frame, text="🚀 SERVER MONITOR (ONLINE MODE)", 
                             font=("Segoe UI", 16, "bold"), fg="#ffffff", bg=THEME["bg_main"])
        lbl_title.pack(side="left")
        self.lbl_status = tk.Label(header_frame, text="Waiting...", font=("Consolas", 10), fg="gray", bg=THEME["bg_main"])
        self.lbl_status.pack(side="right", anchor="s")

        progress_frame = tk.Frame(root, bg=THEME["bg_main"])
        progress_frame.pack(fill="x", padx=20, pady=5)
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("green.Horizontal.TProgressbar", foreground=THEME["accent"], background=THEME["accent"], troughcolor="#333333", bordercolor="#333333", lightcolor=THEME["accent"], darkcolor=THEME["accent"])
        self.progress = ttk.Progressbar(progress_frame, style="green.Horizontal.TProgressbar", orient="horizontal", length=100, mode='determinate')
        self.progress.pack(fill="x")

        log_frame = tk.Frame(root, bg=THEME["bg_main"], bd=1, relief="flat")
        log_frame.pack(expand=True, fill="both", padx=20, pady=10)
        self.log_widget = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=("Consolas", 10), bg=THEME["bg_log"], fg=THEME["fg_text"], insertbackground="white", borderwidth=0)
        self.log_widget.pack(expand=True, fill="both")
        
        self.log_widget.tag_config("TIME", foreground=THEME["timestamp"])
        self.log_widget.tag_config("INFO", foreground=THEME["fg_text"])
        self.log_widget.tag_config("SUCCESS", foreground=THEME["success"], font=("Consolas", 10, "bold"))
        self.log_widget.tag_config("WARNING", foreground=THEME["warning"])
        self.log_widget.tag_config("ERROR", foreground=THEME["error"], font=("Consolas", 10, "bold"))
        self.log_widget.tag_config("HEADER", foreground=THEME["header"], font=("Consolas", 11, "bold"))
        self.log_widget.tag_config("FILE", foreground=THEME["file_tag"], font=("Consolas", 10, "italic"))

        sys.stdout = self
        sys.stderr = self

    def write(self, message):
        if isinstance(message, bytes):
            try: message = message.decode('utf-8', errors='replace')
            except: message = str(message)
        if not message: return
        timestamp = datetime.now().strftime("%H:%M:%S")
        tag = "INFO"
        clean = message.strip()
        if "Error" in clean or "Lỗi" in clean or "Crash" in clean: tag = "ERROR"
        elif "Success" in clean or "OK" in clean or "Running" in clean: tag = "SUCCESS"
        elif "Down" in clean or "Tải" in clean or "Load" in clean: tag = "WARNING"
        elif "BƯỚC" in clean or "===" in clean: tag = "HEADER"
        self.root.after(0, self._append_log, timestamp, message, tag)

    def _append_log(self, timestamp, message, tag):
        self.log_widget.configure(state='normal')
        if message.strip():
             last_char = self.log_widget.get("end-2c", "end-1c")
             if last_char == "\n" or last_char == "":
                self.log_widget.insert(tk.END, f"[{timestamp}] ", "TIME")
        
        self.log_widget.insert(tk.END, message, tag)
        self.log_widget.see(tk.END)
        self.log_widget.configure(state='disabled')

    def flush(self): pass
    def update_progress(self, value, text=None): self.root.after(0, lambda: self._update_prog_ui(value, text))
    def _update_prog_ui(self, value, text):
        self.progress['value'] = value
        if text: self.lbl_status.config(text=text)

def get_base_path():
    if getattr(sys, 'frozen', False): return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))

def load_and_run(app_gui):
    base_path = get_base_path()
    app_gui.update_progress(5, "Khởi động Public Server...")
    print("\n==================================================")
    print("   🌐 CHẾ ĐỘ PUBLIC: CHO PHÉP TRUY CẬP TỪ XA")
    print("==================================================")

    app_gui.update_progress(10, "Load modules...")
    files = {"module_1": "1_map.py", "module_2": "2_map.py"}
    modules = {}

    for name, fname in files.items():
        fpath = os.path.join(base_path, fname)
        if not os.path.exists(fpath):
            print(f"❌ Lỗi: Thiếu file {fname}")
            return
        try:
            print(f"📄 Nạp: {fname}...")
            spec = importlib.util.spec_from_file_location(name, fpath)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            spec.loader.exec_module(mod)
            modules[name] = mod
            print(f"✅ OK: {fname}")
            app_gui.update_progress(app_gui.progress['value'] + 10, f"Đã nạp {fname}")
        except Exception as e:
            print(f"❌ Crash {fname}: {e}")
            return

    print("\n⬇️  BƯỚC 2: TẢI DATA...")
    app_gui.update_progress(40, "Đang tải dữ liệu...")
    
    for name, mod in modules.items():
        if hasattr(mod, "load_all_data"):
            try:
                mod.load_all_data() 
            except Exception as e:
                print(f"⚠️ Warning {name}: {e}")

    app_gui.update_progress(70, "Cấu hình Router...")
    print("\n🔗 BƯỚC 3: MỞ CỔNG 0.0.0.0...")
    
    try:
        master_app = modules["module_1"].app
        slave_app = modules["module_2"].app
        CORS(master_app) 

        @master_app.route('/')
        def index():
            html_path = "Dashboard_Live.html"
            if os.path.exists(os.path.join(base_path, html_path)):
                return send_from_directory(base_path, html_path)
            return "<h1>⚠️ Server chạy OK nhưng thiếu file Dashboard_Live.html</h1>"

        count = 0
        for rule in slave_app.url_map.iter_rules():
            if rule.endpoint != 'static':
                func = slave_app.view_functions[rule.endpoint]
                ep = f"mod2_{rule.endpoint}"
                try:
                    master_app.add_url_rule(rule.rule, endpoint=ep, view_func=func, methods=rule.methods)
                    count += 1
                except: pass
        print(f"✅ Đã Merge {count} API.")
        
    except Exception as e:
        print(f"❌ Setup Error: {e}")
        return

    app_gui.update_progress(90, "Opening Browser...")
    print("\n🌍 BƯỚC 4: ONLINE...")
    
    # Mở local để Đại ca check trước
    html_file = os.path.join(base_path, "Dashboard_Live.html")
    if os.path.exists(html_file):
        webbrowser.open(f"http://localhost:5000") # Mở qua HTTP để test giống client
        print("✅ Đã mở tab kiểm tra Local.")
    
    app_gui.update_progress(100, "SERVER ONLINE (0.0.0.0:5000)")
    print("\n⚡ SERVER IS LIVE ON PUBLIC NETWORK!")
    print("👉 Đại ca dùng IP VPS để truy cập từ máy khác.")

    # QUAN TRỌNG: host='0.0.0.0' để mở truy cập Lan/Wan
    try:
        master_app.run(host='0.0.0.0', debug=False, port=5000, use_reloader=False)
    except Exception as e:
        print(f"❌ Server Crash: {e}")

def main():
    root = tk.Tk()
    app = ModernApp(root)
    t = threading.Thread(target=load_and_run, args=(app,))
    t.daemon = True
    root.after(1000, t.start)
    root.mainloop()

if __name__ == "__main__":
    main()