"""Security scan: assert no audit target contains the real AppID, print only PASS/FAIL."""
import sys
from pathlib import Path

repo = Path(__file__).resolve().parents[2]
env_value = ""
for line in (repo / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line.startswith("TRIMET_APP_ID"):
        env_value = line.partition("=")[2].strip().strip('"').strip("'")
if not env_value:
    print("FAIL: could not read TRIMET_APP_ID")
    sys.exit(2)

targets = [repo / "tools" / "m0_audit" / "evidence"]
targets.append(repo / "tools" / "m0_audit" / "audit_m0.py")
targets.append(repo / "docs" / "trimet-api-notes.md")

checked = 0
leaks = 0
for t in targets:
    files = [t] if t.is_file() else list(t.glob("*"))
    for f in files:
        if not f.is_file():
            continue
        checked += 1
        content = f.read_text(encoding="utf-8", errors="ignore")
        if env_value in content:
            leaks += 1
            print(f"LEAK: {f.name}")
print(f"SECURITY: {checked} files checked, {leaks} leaks")
sys.exit(0 if leaks == 0 else 1)
