"""
Нумерация договоров поставки + общие шрифты для PDF-генераторов бота «Эф».

Сборка PDF договора из бота УБРАНА 12.08.2026 (решение собственника: «бот договор
уже не должен делать»). Договоры выпускаются только в сервисе «Документы»
(дашборд менеджера → «Сервисы» → «Документы», приложение f2b-fishki,
модуль docs_contract.py) — там подписант берётся из карточки МойСклад
в именительном падеже, а родительный для преамбулы «в лице …» считается отдельно.

Что осталось и кем используется:
- get_contract_number() — bot.py, присвоение № при одобрении документа из сервиса
  (общий пул номеров с сервисом, таблица contracts);
- FONT_NORMAL / FONT_BOLD — route_registry.py, driver_checklist.py.

История: plans/2026-08-12-глушим-генерацию-договоров-в-боте.md
"""

import os
import sys
import subprocess

# Автоустановка если нет
try:
    import reportlab
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "reportlab", "--break-system-packages", "-q"])

import glob
from datetime import datetime


# Автопоиск шрифтов DejaVu
def _find_font(name):
    patterns = [
        f"/usr/share/fonts/**/{name}",
        f"/usr/local/share/fonts/**/{name}",
        f"/app/.fonts/{name}",
        f"/home/**/{name}",
    ]
    for p in patterns:
        found = glob.glob(p, recursive=True)
        if found:
            return found[0]
    return None

_FONT = _find_font("DejaVuSans.ttf")
_FONT_BOLD = _find_font("DejaVuSans-Bold.ttf")
_FONT_ITALIC = _find_font("DejaVuSans-Oblique.ttf")

# Если не найдены — скачиваем
if not _FONT:
    import urllib.request, zipfile, io as _io
    url = "https://downloads.sourceforge.net/project/dejavu/dejavu/2.37/dejavu-fonts-ttf-2.37.zip"
    try:
        os.makedirs("/tmp/dejavu", exist_ok=True)
        data = urllib.request.urlopen(url, timeout=30).read()
        with zipfile.ZipFile(_io.BytesIO(data)) as z:
            for name in z.namelist():
                if name.endswith(".ttf") and "DejaVuSans" in name and "/" not in name.replace("dejavu-fonts-ttf-2.37/ttf/", ""):
                    fname = os.path.basename(name)
                    with open(f"/tmp/dejavu/{fname}", "wb") as f:
                        f.write(z.read(name))
        _FONT = "/tmp/dejavu/DejaVuSans.ttf"
        _FONT_BOLD = "/tmp/dejavu/DejaVuSans-Bold.ttf"
        _FONT_ITALIC = "/tmp/dejavu/DejaVuSans-Oblique.ttf"
    except Exception as e:
        # Fallback — используем встроенные шрифты reportlab без кириллицы
        _FONT = None

# Регистрируем шрифты
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

if _FONT and os.path.exists(_FONT):
    pdfmetrics.registerFont(TTFont("DejaVu", _FONT))
    pdfmetrics.registerFont(TTFont("DejaVuBold", _FONT_BOLD or _FONT))
    pdfmetrics.registerFont(TTFont("DejaVuItalic", _FONT_ITALIC or _FONT))
    FONT_NORMAL = "DejaVu"
    FONT_BOLD = "DejaVuBold"
else:
    # Встроенный шрифт (без кириллицы — крайний случай)
    FONT_NORMAL = "Helvetica"
    FONT_BOLD = "Helvetica-Bold"


def get_contract_number(date: datetime, db) -> str:
    """Генерирует номер договора на основе даты. Если уже есть — добавляет /2, /3 и т.д."""
    base = date.strftime("%d%m%y")
    existing = db._fetchall(
        "SELECT contract_number FROM contracts WHERE contract_number LIKE %s",
        (f"{base}%",)
    )
    if not existing:
        return base
    return f"{base}/{len(existing) + 1}"
