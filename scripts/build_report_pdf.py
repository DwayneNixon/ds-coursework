"""Generate a readable, <=8-page draft PDF from docs/REPORT_DRAFT.md.

Run from repo root AFTER X1, X2 and X4:
    python -m pip install -r requirements-report.txt
    python -m scripts.build_report_pdf

Output: submission/parameter_server_report.pdf
The recorded video still has to be made by the student.
"""
import argparse
import csv
import html
import re
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def make_x1_plot(root, destination):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    rows = list(csv.DictReader(
        (root / "results/x1_aggregation/oracle.csv").open(encoding="utf-8")))
    x = [int(r["iteration"]) for r in rows]
    predicted = [float(r["expected_distance"]) for r in rows]
    measured = [float(r["measured_distance"]) for r in rows]
    fig, ax = plt.subplots(figsize=(8, 3))
    ax.plot(x, predicted, label="Analytic oracle", linewidth=2)
    ax.scatter(x, measured, s=17, marker="x", label="Measured")
    ax.set(xlabel="Completed synchronous rounds",
           ylabel="Distance to target",
           title="X1 — multiworker contraction")
    ax.grid(alpha=.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(destination, dpi=165)
    plt.close(fig)


def architecture_figure(width):
    from reportlab.graphics.shapes import Drawing, Rect, String, Line
    from reportlab.lib import colors
    drawing = Drawing(width, 123)
    blocks = [
        (8, 73, 91, "Worker emulator"),
        (116, 73, 97, "Coordinator"),
        (230, 73, 108, "Distributed client"),
        (355, 73, 145, "Primary shards / gRPC"),
        (230, 14, 108, "Manager / epochs"),
        (355, 14, 145, "Replicas / recovery"),
    ]
    for x, y, w, name in blocks:
        drawing.add(Rect(x, y, w, 33, strokeColor=colors.HexColor("#3C536A"),
                         fillColor=colors.HexColor("#EEF3F7"),
                         strokeWidth=0.8, rx=5, ry=5))
        drawing.add(String(x + w/2, y + 13, name, fontName="Helvetica",
                           fontSize=7.1, textAnchor="middle",
                           fillColor=colors.HexColor("#1F3446")))
    for x1, x2 in ((99,116),(213,230),(338,355)):
        drawing.add(Line(x1,89,x2,89,strokeColor=colors.HexColor("#2F719D"),
                         strokeWidth=1.5))
    drawing.add(Line(284,73,284,47,strokeColor=colors.HexColor("#2F719D")))
    drawing.add(Line(283,47,283,14+33,strokeColor=colors.HexColor("#2F719D")))
    drawing.add(Line(427,73,427,47,strokeColor=colors.HexColor("#2F719D")))
    drawing.add(Line(427,47,427,14+33,strokeColor=colors.HexColor("#2F719D")))
    drawing.add(String(8,113,"Synchronous / bounded / eventual · task IDs · range clocks",
                       fontSize=8.2, fillColor=colors.HexColor("#516679")))
    return drawing


def inline_markup(s, fontname):
    # Work only with text supported by ReportLab's Paragraph subset.
    s = html.escape(s.strip())
    s = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", s)
    s = re.sub(r"`([^`]+)`",
               lambda m: f'<font name="{fontname}" color="#124B70">'
                         + m.group(1) + "</font>", s)
    s = re.sub(r"\*([^*]+)\*", r"<i>\1</i>", s)
    s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", s)
    return s


def parse_markdown(markdown):
    lines = markdown.splitlines()
    blocks, paragraph = [], []
    def flush():
        if paragraph:
            blocks.append(("para", " ".join(paragraph)))
            paragraph.clear()
    i = 0
    while i < len(lines):
        raw = lines[i].strip()
        if not raw:
            flush()
            i += 1
            continue
        if raw.startswith("|"):
            flush()
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                parts = [x.strip() for x in lines[i].strip().strip("|").split("|")]
                if not all(re.match(r"^:?-{3,}:?$", part) for part in parts):
                    rows.append(parts)
                i += 1
            blocks.append(("table", rows))
            continue
        if raw.startswith("#"):
            flush()
            level = len(raw) - len(raw.lstrip("#"))
            blocks.append(("heading", (level,raw[level:].strip())))
            i += 1
            continue
        if raw.startswith(">"):
            flush()
            blocks.append(("quote",raw[1:].strip()))
            i += 1
            continue
        if raw.startswith("- "):
            flush()
            blocks.append(("bullet",raw[2:]))
            i += 1
            continue
        paragraph.append(raw)
        i += 1
    flush()
    return blocks


def build_document(source, destination, figure_paths, font_size):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image, KeepTogether,
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    fontfile = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
    if fontfile.exists():
        try:
            pdfmetrics.registerFont(TTFont("DS-DejaVu", str(fontfile)))
        except Exception:
            pass
    name = "DS-DejaVu" if "DS-DejaVu" in pdfmetrics.getRegisteredFontNames() else "Helvetica"
    normal = ParagraphStyle("normal", fontName=name, fontSize=font_size,
                            leading=font_size*1.35, textColor=colors.HexColor("#253441"),
                            spaceAfter=5)
    heading = {
        1: ParagraphStyle("h1", parent=normal, fontSize=17, leading=21,
                          spaceBefore=5, spaceAfter=11, textColor=colors.HexColor("#153B55")),
        2: ParagraphStyle("h2", parent=normal, fontSize=12.4, leading=16,
                          spaceBefore=13, spaceAfter=6, keepWithNext=True,
                          textColor=colors.HexColor("#153B55")),
        3: ParagraphStyle("h3", parent=normal, fontSize=font_size+1.0,
                          leading=font_size*1.5, spaceBefore=8,
                          spaceAfter=4, keepWithNext=True,
                          textColor=colors.HexColor("#2D5B74")),
    }
    quote_style = ParagraphStyle("quote",parent=normal,fontSize=font_size-0.4,
                                  leftIndent=10,textColor=colors.HexColor("#65747B"))
    caption_style = ParagraphStyle("caption", parent=normal,
                                   fontSize=font_size-1.0, leading=font_size*1.2,
                                   alignment=TA_CENTER,
                                   textColor=colors.HexColor("#667989"),
                                   spaceAfter=7)
    bullets = ParagraphStyle("bullets",parent=normal,leftIndent=12,firstLineIndent=-8)
    page_width, _ = A4
    available = page_width - 2*40
    blocks = parse_markdown(source)
    story = []
    active_figure = None
    included = set()
    for kind, data in blocks:
        if kind == "heading":
            level, value = data
            if level > 3:
                level = 3
            story.append(Paragraph(inline_markup(value,name),heading[level]))
            if level == 2 and value.startswith("2. Architecture"):
                story.append(architecture_figure(available))
                story.append(Paragraph(
                    "Figure 1. Implementation overview: worker, coordinator, "
                    "routed primary shards, ownership manager and recovery.",
                    caption_style))
            if level == 3:
                matched = re.match(r"X([1-7]):",value)
                active_figure = int(matched.group(1)) if matched else None
        elif kind == "table":
            if not data:
                continue
            n = len(data[0])
            cols = [available / n] * n
            cells = []
            for row in data:
                cells.append([
                    Paragraph(inline_markup(v,name), ParagraphStyle(
                        "table",parent=normal,fontSize=max(6.4,font_size-1.2),
                        leading=max(8.2,font_size*1.15),spaceAfter=0))
                    for v in (row+[""]*n)[:n]
                ])
            tb = Table(cells,colWidths=cols,repeatRows=1,hAlign="LEFT")
            tb.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#DEEAF3")),
                ("TEXTCOLOR",(0,0),(-1,0),colors.HexColor("#17364C")),
                ("VALIGN",(0,0),(-1,-1),"TOP"),
                ("TOPPADDING",(0,0),(-1,-1),4),
                ("BOTTOMPADDING",(0,0),(-1,-1),4),
                ("LEFTPADDING",(0,0),(-1,-1),4),
                ("RIGHTPADDING",(0,0),(-1,-1),4),
                ("LINEBELOW",(0,0),(-1,0),0.6,colors.HexColor("#A4B7C4")),
                ("LINEBELOW",(0,1),(-1,-1),0.2,colors.HexColor("#DFE7EC")),
            ]))
            story.extend([tb,Spacer(1,6)])
        elif kind == "quote":
            # Submission PDF should not carry internal draft/pagination reminders.
            continue
        elif kind == "para":
            if data.startswith("**Status:**") or data.startswith("**Pagination:**"):
                continue
            if data.startswith("**Figure 1 (insert):**"):
                continue
            story.append(Paragraph(inline_markup(data,name), normal))
            if active_figure is not None and active_figure in figure_paths and active_figure not in included:
                fp=figure_paths[active_figure]
                if fp.exists():
                    img=Image(str(fp))
                    scale=min(available/img.imageWidth, 182/img.imageHeight)
                    img.drawWidth=img.imageWidth*scale
                    img.drawHeight=img.imageHeight*scale
                    story.append(Spacer(1,3))
                    story.append(img)
                    story.append(Paragraph(f"Figure {active_figure+1}. "
                        + {1:"X1 analytic versus measured contraction",
                           2:"X2 median throughput and p99 latency",
                           3:"X3 convergence under stragglers",
                           4:"X4 application payload comparison",
                           5:"X5 live throughput during migration"}[active_figure],
                        caption_style))
                    included.add(active_figure)
                active_figure=None
        elif kind=="bullet":
            story.append(Paragraph("• "+inline_markup(data,name),bullets))
    def page_footer(canvas,doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#CBD6DD"))
        canvas.line(40,32,page_width-40,32)
        canvas.setFont("Helvetica",7.4)
        canvas.setFillColor(colors.HexColor("#667989"))
        canvas.drawString(40,20,"Distributed Systems · Parameter Server · University of Edinburgh")
        canvas.drawRightString(page_width-40,20,f"{doc.page}")
        canvas.restoreState()
    doc = SimpleDocTemplate(str(destination),pagesize=A4,
                            leftMargin=40,rightMargin=40,topMargin=38,
                            bottomMargin=43,title="Building a Parameter Server — Distributed Systems",
                            author="Coursework project team")
    doc.build(story,onFirstPage=page_footer,onLaterPages=page_footer)


def main():
    import matplotlib
    matplotlib.use("Agg")
    from pypdf import PdfReader
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source",default=str(ROOT/"docs/REPORT_DRAFT.md"))
    parser.add_argument("--output",default=str(ROOT/"submission/parameter_server_report.pdf"))
    args=parser.parse_args()
    source=Path(args.source).read_text(encoding="utf-8")
    output=Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ds_report_") as scratch:
        x1=Path(scratch)/"x1.png"
        make_x1_plot(ROOT,x1)
        figure_paths={
            1:x1,
            2:ROOT/"results/x2_scaling/scaling.png",
            3:ROOT/"results/c5_evaluation/convergence_by_updates_60ms.png",
            4:ROOT/"results/x4_comparison/comparison.png",
            5:ROOT/"results/e4_elasticity/trial_20261121_150052_626785/throughput.png",
        }
        for name,path in figure_paths.items():
            if not path.is_file():
                raise FileNotFoundError(f"Missing figure {name}: {path}")
        for font_size in (9.2,8.8,8.4,8.1):
            build_document(source,output,figure_paths,font_size)
            count=len(PdfReader(str(output)).pages)
            if count<=8:
                print(f"Report PDF: {count} pages at {font_size}pt — PASS")
                print(f"Output: {output}")
                return
        raise RuntimeError(f"PDF is {count} pages after compression: limit 8")


if __name__=="__main__":
    main()
