"""
Генератор акта сверки взаиморасчётов PDF.
"""
import os, glob
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import logging
_log = logging.getLogger(__name__)

# ── Шрифты (та же логика что в contract_generator) ────────────────────────────
def _find_font(name):
    for p in [f"/usr/share/fonts/**/{name}", f"/usr/local/share/fonts/**/{name}",
              f"/app/.fonts/{name}", f"/home/**/{name}"]:
        found = glob.glob(p, recursive=True)
        if found: return found[0]
    return None

_FONT      = _find_font("DejaVuSans.ttf")
_FONT_BOLD = _find_font("DejaVuSans-Bold.ttf")

if not _FONT:
    import urllib.request, zipfile, io as _io2
    try:
        os.makedirs("/tmp/dejavu", exist_ok=True)
        data = urllib.request.urlopen(
            "https://downloads.sourceforge.net/project/dejavu/dejavu/2.37/dejavu-fonts-ttf-2.37.zip",
            timeout=30).read()
        with zipfile.ZipFile(_io2.BytesIO(data)) as z:
            for nm in z.namelist():
                if nm.endswith(".ttf") and "DejaVuSans" in nm and "/" not in nm.replace("dejavu-fonts-ttf-2.37/ttf/", ""):
                    with open(f"/tmp/dejavu/{os.path.basename(nm)}", "wb") as f:
                        f.write(z.read(nm))
        _FONT      = "/tmp/dejavu/DejaVuSans.ttf"
        _FONT_BOLD = "/tmp/dejavu/DejaVuSans-Bold.ttf"
    except Exception as e:
        _log.warning(f"Font download failed: {e}")
        _FONT = None

if _FONT and os.path.exists(_FONT):
    try:
        pdfmetrics.registerFont(TTFont("RecF",  _FONT))
        pdfmetrics.registerFont(TTFont("RecFB", _FONT_BOLD or _FONT))
        F, FB = "RecF", "RecFB"
    except Exception:
        F, FB = "Helvetica", "Helvetica-Bold"
else:
    F, FB = "Helvetica", "Helvetica-Bold"

# ── Assets ────────────────────────────────────────────────────────────────────
def _find_asset(name):
    for d in [os.path.dirname(os.path.abspath(__file__)), "/app"]:
        for sub in ["assets/", ""]:
            p = os.path.join(d, sub + name)
            if os.path.exists(p): return p
    return None

def generate_reconciliation_pdf(data: dict) -> bytes:
    import io
    sign  = _find_asset("podpis.png")
    stamp = _find_asset("pechat.png")
    _log.info(f"reconciliation_generator: font={F} sign={sign} stamp={stamp}")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=15*mm, bottomMargin=30*mm)

    n  = ParagraphStyle("n",  fontName=F,  fontSize=8,  leading=11)
    b  = ParagraphStyle("b",  fontName=FB, fontSize=8,  leading=11)
    t  = ParagraphStyle("t",  fontName=FB, fontSize=11, leading=15, alignment=TA_CENTER)
    s  = ParagraphStyle("s",  fontName=F,  fontSize=7,  leading=10)
    r  = ParagraphStyle("r",  fontName=F,  fontSize=8,  leading=11, alignment=TA_RIGHT)
    br = ParagraphStyle("br", fontName=FB, fontSize=8,  leading=11, alignment=TA_RIGHT)
    W  = A4[0] - 30*mm

    cp_name      = data.get("counterparty_name", "")
    date_from    = data.get("date_from", "")
    date_to      = data.get("date_to", "")
    rows         = data.get("rows", [])
    debit_total  = data.get("debit_total", 0)
    credit_total = data.get("credit_total", 0)
    closing      = data.get("closing_balance", 0)

    def fd(d):
        try: return datetime.strptime(d, "%Y-%m-%d").strftime("%d.%m.%Y")
        except: return d
    def fm(v): return f"{v:,.2f}".replace(",", " ")

    story = []
    story.append(Paragraph("АКТ СВЕРКИ ВЗАИМОРАСЧЁТОВ", t))
    story.append(Paragraph(f"за период с {fd(date_from)} по {fd(date_to)}",
        ParagraphStyle("sub", fontName=F, fontSize=9, leading=13, alignment=TA_CENTER)))
    story.append(Spacer(1, 3*mm))

    buyer_info = f"<b>{cp_name}</b>"
    if data.get("buyer_inn"): buyer_info += f"<br/>ИНН: {data['buyer_inn']}"
    if data.get("buyer_address"): buyer_info += f"<br/>{data['buyer_address']}"

    parties = Table([[
        Paragraph("<b>АО «ФИШ ТУ БИЗНЕС»</b><br/>ИНН: 9713025854 · ОГРН: 1257700150553<br/>"
                  "127238, Москва, проезд 3-й Нижнелихоборский, д. 1А<br/>Тел.: +7 800 700 27 03", s),
        Paragraph(buyer_info, s),
    ]], colWidths=[W/2, W/2])
    parties.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),("BOX",(0,0),(-1,-1),0.5,colors.grey),
        ("INNERGRID",(0,0),(-1,-1),0.5,colors.grey),
        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),("LEFTPADDING",(0,0),(-1,-1),4),
    ]))
    story.append(parties)
    story.append(Spacer(1, 3*mm))

    col_w = [20*mm, 35*mm, 85*mm, 27*mm, 27*mm]
    tdata = [[Paragraph("<b>Дата</b>",b), Paragraph("<b>Тип</b>",b),
              Paragraph("<b>Документ</b>",b),
              Paragraph("<b>Дебет</b><br/><i>отгрузка</i>",b),
              Paragraph("<b>Кредит</b><br/><i>оплата</i>",b)]]
    for row in rows:
        d = fm(row["amount"]) if not row["is_payment"] else ""
        c = fm(row["amount"]) if row["is_payment"] else ""
        tdata.append([Paragraph(fd(row["date"]),n), Paragraph(row["doc_type"],n),
                      Paragraph(row["doc_number"][:60],n), Paragraph(d,r), Paragraph(c,r)])
    tdata.append([Paragraph("<b>ИТОГО:</b>",b),Paragraph("",n),Paragraph("",n),
                  Paragraph(f"<b>{fm(debit_total)}</b>",br), Paragraph(f"<b>{fm(credit_total)}</b>",br)])

    ops = Table(tdata, colWidths=col_w, repeatRows=1)
    ops.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#E8F0FE")),
        ("BACKGROUND",(0,-1),(-1,-1),colors.HexColor("#F5F5F5")),
        ("BOX",(0,0),(-1,-1),0.5,colors.grey),
        ("INNERGRID",(0,0),(-1,-1),0.3,colors.HexColor("#CCCCCC")),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),2),("LEFTPADDING",(0,0),(-1,-1),3),
    ]))
    story.append(ops)
    story.append(Spacer(1, 4*mm))

    if closing > 0:
        saldo = f"Задолженность <b>{cp_name}</b> перед АО «ФИШ ТУ БИЗНЕС»: <b>{fm(closing)} руб.</b>"
    elif closing < 0:
        saldo = f"Задолженность АО «ФИШ ТУ БИЗНЕС» перед <b>{cp_name}</b>: <b>{fm(abs(closing))} руб.</b>"
    else:
        saldo = "Взаиморасчёты <b>согласованы</b>. Задолженность отсутствует."
    story.append(Paragraph(saldo, ParagraphStyle("saldo", fontName=F, fontSize=9, leading=13)))
    story.append(Spacer(1, 40*mm))

    def _draw(cv, dc):
        cv.saveState()
        lx = dc.leftMargin
        rx = lx + (A4[0] - lx - dc.rightMargin) / 2
        by = 55*mm
        cv.setFont(FB,8); cv.drawString(lx, by+16*mm, "От АО «ФИШ ТУ БИЗНЕС»:")
        cv.setFont(F,8);  cv.drawString(lx, by+11*mm, "Генеральный Директор")
        cv.line(lx, by+5*mm, lx+65*mm, by+5*mm)
        if sign and os.path.exists(sign):
            cv.drawImage(sign, lx-65*mm, by-22*mm-10*mm, width=151*mm, height=65*mm,
                         mask="auto", preserveAspectRatio=True)
        if stamp and os.path.exists(stamp):
            cv.drawImage(stamp, lx+2*mm, by-22*mm, width=55*mm, height=55*mm,
                         mask="auto", preserveAspectRatio=True)
        cv.setFont(FB,8); cv.drawString(lx, by, "/Маланчук А.В./")
        cv.setFont(FB,8); cv.drawString(rx, by+16*mm, f"От {cp_name[:40]}:")
        cv.setFont(F,8);  cv.drawString(rx, by+11*mm, "Руководитель")
        cv.line(rx, by+5*mm, rx+65*mm, by+5*mm)
        cv.setFont(FB,8); cv.drawString(rx, by, "М.П.")
        cv.restoreState()

    doc.build(story, onFirstPage=_draw, onLaterPages=_draw)
    return buf.getvalue()
