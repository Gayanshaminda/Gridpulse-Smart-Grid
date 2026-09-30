"""Render the editable report source with explicit evidence status."""
import json
import re
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.utils import ImageReader
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, KeepTogether
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "pdf"
OUT.mkdir(parents=True, exist_ok=True)
evidence_path = ROOT / "output" / "validation.json"
evidence = json.loads(evidence_path.read_text()) if evidence_path.exists() else {
    "full_pipeline": "Not yet verified; do not treat offline dashboard evidence as a distributed-system test.",
    "checks": ["No saved validation record is available. Run the documented checks and record their actual results."],
    "screenshots": "Offline fixture: saved sample readings, not a running Kafka/Spark/Airflow execution."}

styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="BodyGrid", fontName="Helvetica", fontSize=10, leading=14.3, spaceAfter=9, textColor=colors.HexColor("#253347")))
styles.add(ParagraphStyle(name="TitleGrid", fontName="Helvetica-Bold", fontSize=25, leading=29, spaceAfter=18, textColor=colors.HexColor("#102a43")))
styles.add(ParagraphStyle(name="HeadingGrid", fontName="Helvetica-Bold", fontSize=12.5, leading=16, spaceBefore=10, spaceAfter=7, textColor=colors.HexColor("#127a77")))
styles.add(ParagraphStyle(name="CaptionGrid", fontName="Helvetica-Oblique", fontSize=8.5, leading=11, spaceAfter=10, textColor=colors.HexColor("#536577")))


def architecture():
    d = Drawing(475, 198)
    def box(x, y, w, label):
        d.add(Rect(x, y, w, 32, rx=5, fillColor=colors.HexColor("#eaf5f3"), strokeColor=colors.HexColor("#127a77")))
        d.add(String(x+w/2, y+12, label, textAnchor="middle", fontName="Helvetica", fontSize=8))
    def arrow(x1,y1,x2,y2):
        d.add(Line(x1,y1,x2,y2,strokeColor=colors.HexColor("#537082")))
        if x2>x1: d.add(Polygon([x2,y2,x2-5,y2+3,x2-5,y2-3],fillColor=colors.HexColor("#537082"),strokeColor=None))
    box(0,155,96,"Meter simulator")
    box(121,155,82,"Kafka")
    box(228,155,114,"Spark streaming")
    box(369,155,103,"Live readings")
    arrow(96,171,121,171);arrow(203,171,228,171);arrow(342,171,369,171)
    box(228,93,114,"Raw Parquet")
    d.add(Line(285,155,285,125))
    box(0,30,96,"Daily tariff CSV")
    box(0,93,96,"Airflow")
    box(228,30,114,"Spark batch")
    box(369,30,103,"Daily bills")
    arrow(96,46,228,46);arrow(96,109,228,62)
    d.add(Line(285,93,285,62));arrow(342,46,369,46)
    d.add(String(237,4,"PostgreSQL serves dashboard + API",fontName="Helvetica-Bold",fontSize=9,textAnchor="middle"))
    return d


def footer(canvas, doc):
    canvas.setStrokeColor(colors.HexColor("#d4dfe7"))
    canvas.line(48, 40, 547, 40)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#526779"))
    canvas.drawString(48, 27, "GRIDPULSE  |  EC8203  |  Implementation report draft")
    canvas.drawRightString(547, 27, str(doc.page))


story = []
source = (ROOT / "docs" / "REPORT.md").read_text(encoding="utf-8")
source = source.replace("<!-- page -->", "\n\n<!-- page -->\n\n")
for block in re.split(r"\n\s*\n", source):
    block = block.strip()
    if not block: continue
    if block == "<!-- page -->":
        story.append(PageBreak()); continue
    if block == "{{architecture}}":
        story.extend([architecture(), Spacer(1,12)]); continue
    if block in ("{{screenshot_live}}", "{{screenshot_billing}}"):
        name = "live-energy.png" if "live" in block else "daily-billing.png"
        path = ROOT / "output" / "screenshots" / name
        if path.exists():
            width, height = ImageReader(str(path)).getSize()
            img = Image(str(path), width=410, height=height * 410 / width)
            story.extend([img, Spacer(1, 8)])
        continue
    if block == "{{screenshot_caption}}":
        story.append(Paragraph(escape(evidence["screenshots"]), styles["CaptionGrid"])); continue
    if block == "{{validation}}":
        story.append(Paragraph(escape(evidence["full_pipeline"]), styles["BodyGrid"]))
        for check in evidence["checks"]:
            story.append(Paragraph("- " + escape(check), styles["BodyGrid"]))
        continue
    if block.startswith("# "):
        # A block may contain a title and immediate subtitle.
        for line in block.splitlines():
            heading = "TitleGrid" if line.startswith("# ") else "HeadingGrid"
            story.append(Paragraph(escape(line.lstrip("# ")), styles[heading]))
    elif block.startswith("## "):
        story.append(Paragraph(escape(block[3:]), styles["HeadingGrid"]))
    else:
        story.append(Paragraph(escape(block).replace("\n", " "), styles["BodyGrid"]))

doc = SimpleDocTemplate(str(OUT / "GridPulse_Report.pdf"), pagesize=(595.28, 841.89),
                        rightMargin=48, leftMargin=48, topMargin=43, bottomMargin=53,
                        title="GridPulse - Smart Grid Energy Monitoring and Daily Billing", author="EC8203 project team")
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print(OUT / "GridPulse_Report.pdf")
