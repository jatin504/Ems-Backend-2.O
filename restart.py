import os
import sys
import time
import subprocess
import psutil  # pip install psutil

def find_and_kill(proc_name="main.py"):
    """Find and kill any running Python process running main.py"""
    current_pid = os.getpid()
    killed = False

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # Check if it's a Python process running main.py
            if proc.info['cmdline']:
                cmdline = proc.info['cmdline']
                if any("python" in arg.lower() for arg in cmdline) and "main.py" in ' '.join(cmdline):
                    pid = proc.info['pid']
                    if pid != current_pid:
                        print(f"🔁 Found old process: PID {pid}")
                        proc.kill()
                        killed = True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    if killed:
        print("⏳ Waiting 2 seconds for clean shutdown...")
        time.sleep(2)

def start_main():
    """Start main.py in a new process"""
    print("🚀 Starting main.py...")
    if sys.platform == "win32":
        # On Windows
        subprocess.Popen([sys.executable, "main.py"], creationflags=subprocess.CREATE_NEW_CONSOLE)
    else:
        # On Linux/macOS
        subprocess.Popen([sys.executable, "main.py"])

if __name__ == "__main__":
    find_and_kill()
    # start_main()