"""Create a submission archive containing source, report and validation evidence."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root = Path(__file__).resolve().parents[1]
output = root / "output"
output.mkdir(exist_ok=True)
target = output / "GridPulse_Submission.zip"
folders = ["grid", "dashboard", "dags", "tests", "scripts", "sql", "docs", ".streamlit"]
files = [root / name for name in ["README.md", "Dockerfile", "Dockerfile.airflow", "compose.yaml",
                                "requirements.txt", "requirements-report.txt", ".env.example", ".gitignore", ".dockerignore"]]
for name in folders:
    files.extend(p for p in (root / name).rglob("*") if p.is_file() and "__pycache__" not in p.parts)
for name in ["pdf/GridPulse_Report.pdf", "validation.json", "smoke_result.json", "test-results.txt"]:
    path = output / name
    if path.exists(): files.append(path)
files.extend((output / "screenshots").glob("*.png"))
with ZipFile(target, "w", compression=ZIP_DEFLATED) as archive:
    for path in sorted(set(files)):
        archive.write(path, "GridPulse/" + path.relative_to(root).as_posix())
print(f"Created {target} ({target.stat().st_size:,} bytes)")
