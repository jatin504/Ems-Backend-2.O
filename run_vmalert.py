"""Run the installed vmalert executable using repository .env settings."""
import os
import subprocess
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv()


def env_path(name):
    """Resolve relative paths from the project root."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing {name} in .env")

    path = Path(value).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()

def main():
    """Launch vmalert in the foreground; expose its logs and actual exit status."""
    executable = env_path("VMALERT_EXE")
    rules = env_path("VMALERT_RULES_FILE")
    if not executable.is_file():
        raise FileNotFoundError(f"VMALERT_EXE does not exist: {executable}")
    if not rules.is_file():
        raise FileNotFoundError(f"Rules file missing: {rules}. Run run_rule_generator.py --once --no-reload first.")

    # Rule evaluation reads and writes the same VM deployment by default.
    vm_url = os.getenv("VMALERT_DATASOURCE_URL")
    command = [str(executable), f"-rule={rules}", f"-datasource.url={vm_url}",
               f"-remoteWrite.url={vm_url}",
               f"-httpListenAddr={os.getenv('VMALERT_LISTEN_ADDR', ':8880')}"]
    # Argument lists preserve paths containing spaces; no shell or extra window.
    process = subprocess.Popen(command, cwd=PROJECT_ROOT)
    try:
        return process.wait()
    except KeyboardInterrupt:
        # Do not leave the child server running when this launcher is stopped.
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
