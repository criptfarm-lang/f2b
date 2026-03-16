"""
Генератор договора поставки АО «ФИШ ТУ БИЗНЕС» в PDF.
Подставляет данные покупателя, номер и дату договора.
Вставляет подпись и печать поставщика.
"""

import os
import io
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, HRFlowable
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib import colors

# Регистрируем шрифты
FONT_PATH = "/usr/share/fonts/truetype/dejavu/"
pdfmetrics.registerFont(TTFont("DejaVu", FONT_PATH + "DejaVuSans.ttf"))
pdfmetrics.registerFont(TTFont("DejaVuBold", FONT_PATH + "DejaVuSans-Bold.ttf"))
pdfmetrics.registerFont(TTFont("DejaVuItalic", FONT_PATH + "DejaVuSans-Oblique.ttf"))

# Пути к ресурсам (будут подставлены при деплое)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGO_PATH = os.path.join(BASE_DIR, "assets", "logo.png")
SIGN_PATH = os.path.join(BASE_DIR, "assets", "podpis.png")
STAMP_PATH = os.path.join(BASE_DIR, "assets", "pechat.png")

# Стили
def make_styles():
    normal = ParagraphStyle("normal", fontName="DejaVu", fontSize=9,
                            leading=13, spaceAfter=4, alignment=TA_JUSTIFY)
    bold = ParagraphStyle("bold", fontName="DejaVuBold", fontSize=9,
                          leading=13, spaceAfter=4)
    title = ParagraphStyle("title", fontName="DejaVuBold", fontSize=13,
                           leading=18, spaceAfter=6, alignment=TA_CENTER)
    subtitle = ParagraphStyle("subtitle", fontName="DejaVuBold", fontSize=10,
                              leading=14, spaceAfter=4, alignment=TA_CENTER)
    heading = ParagraphStyle("heading", fontName="DejaVuBold", fontSize=9,
                             leading=13, spaceAfter=4, alignment=TA_CENTER)
    small = ParagraphStyle("small", fontName="DejaVu", fontSize=8,
                           leading=11, spaceAfter=2)
    small_bold = ParagraphStyle("small_bold", fontName="DejaVuBold", fontSize=8,
                                leading=11, spaceAfter=2)
    return normal, bold, title, subtitle, heading, small, small_bold


def generate_contract_pdf(data: dict) -> bytes:
    """
    Генерирует PDF договора поставки.

    data: {
        contract_number: str,     # номер договора (напр. "25032601")
        contract_date: str,       # дата (напр. "26 марта 2025 г.")
        buyer_name: str,          # полное наим. (напр. "ООО «Ромашка»")
        buyer_representative: str, # "генерального директора Иванова И.И."
        buyer_inn: str,
        buyer_ogrn: str,
        buyer_address: str,
        buyer_bank: str,
        buyer_rs: str,
        buyer_bik: str,
        buyer_ks: str,
        buyer_phone: str,
        buyer_email: str,
        buyer_director_name: str,  # "Иванов И.И." для подписи
    }
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=15*mm,
        topMargin=15*mm, bottomMargin=15*mm
    )

    normal, bold, title, subtitle, heading, small, small_bold = make_styles()
    story = []
    W = A4[0] - 35*mm  # ширина контента

    # ── Логотип ─────────────────────────────────────────────────────────────
    if os.path.exists(LOGO_PATH):
        img = Image(LOGO_PATH, width=120*mm, height=33*mm)
        story.append(img)
        story.append(Spacer(1, 4*mm))

    # ── Заголовок ────────────────────────────────────────────────────────────
    story.append(Paragraph("ДОГОВОР ПОСТАВКИ", title))
    story.append(Paragraph(
        f"№ {data['contract_number']} от {data['contract_date']}", subtitle))
    story.append(Spacer(1, 3*mm))

    # ── Вводный абзац ────────────────────────────────────────────────────────
    buyer_short = data["buyer_name"]
    buyer_rep = data.get("buyer_representative", "")
    intro = (
        f"Акционерное Общество «ФИШ ТУ БИЗНЕС», именуемое в дальнейшем «Поставщик», "
        f"в лице Генерального Директора Маланчука Александра Владимировича, "
        f"действующего на основании Устава, с одной стороны, и "
        f"{buyer_short}, именуемый в дальнейшем «Покупатель», "
        f"в лице {buyer_rep}, с другой стороны, "
        f"вместе именуемые стороны, заключили настоящий договор о нижеследующем:"
    )
    story.append(Paragraph(intro, normal))
    story.append(Spacer(1, 2*mm))

    # ── Текст договора ───────────────────────────────────────────────────────
    sections = [
        ("1. ПРЕДМЕТ ДОГОВОРА", [
            "1.1. Поставщик обязуется в порядке и на условиях Договора поставлять в собственность Покупателя, а Покупатель принимать и оплачивать Товар, представленный в заказе Покупателя.",
            "1.2. Поставка Товара осуществляется отдельными партиями в течение срока действия Договора. Покупатель передает заказ Поставщику с помощью электронных или иных технических средств, в рабочие дни с 09-00 до 18-00 не позднее одного рабочего дня до дня планируемой даты поставки. Заказ должен содержать количество и ассортимент Товара. Корректировка заказа производится не позднее дня, предшествующего дню поставки. Поставщик имеет право не принимать заказ к исполнению, если у Покупателя имеется просроченная задолженность по оплате за полученный ранее Товар.",
            "1.3. Наименование, количество и цена Товара указываются в УПД. Факт принятия Товара Покупателем от Поставщика (или транспортной организации) в любом случае означает, что Стороны достигли соглашения о наименовании, ассортименте, цене и количестве переданного по настоящему договору Товара.",
        ]),
        ("2. ЦЕНА И ПОРЯДОК РАСЧЕТОВ", [
            "2.1. Цена на Товар установлена в рублях и включает в себя НДС.",
            "2.2. Цена Товара установлена в прайс-листе Поставщика, указывается в УПД, а если Товар предварительно оплачен, то в счете.",
            "2.3. Оплата Товара производится Покупателем в течение 14 (четырнадцати) календарных дней с момента передачи Товара Покупателю. Передача Товара Покупателю признается состоявшейся с момента подписания представителем Покупателя или транспортной (экспедиторской) организации товарно-сопроводительных документов. Покупатель производит оплату поставленного Товара путём внесения наличных денежных средств в кассу Поставщика в пределах лимитов, установленных действующим законодательством РФ, либо безналичным платежом на расчетный счет Поставщика. Моментом оплаты Товара считается дата поступления денежных средств на расчетный счет Поставщика. При наличии у Покупателя просроченной задолженности Поставщик вправе изменить условия расчетов по последующим поставкам, потребовав 100% предоплаты. Все расходы, комиссии банков, связанные с перечислением платежей, несет Покупатель.",
            "2.4. Право собственности и риск случайной гибели на Товар переходит Покупателю: в момент передачи Товара уполномоченному представителю Покупателя на складе Поставщика в случае самовывоза; в момент передачи Товара на склад Покупателя в случае доставки Товара силами Поставщика; в момент передачи Товара уполномоченному представителю Транспортной компании (грузоперевозчику).",
        ]),
        ("3. КАЧЕСТВО ТОВАРОВ", [
            "3.1. Поставщик гарантирует, что качество поставляемого Товара соответствует требованиям стандартов, принятых в Российской Федерации, и обеспечивает безопасность жизни и здоровья потребителей. Качество Товаров подтверждается сертификатом соответствия, ветеринарным свидетельством. Вышеуказанные документы передаются Поставщиком Покупателю единовременно с передачей Товара.",
        ]),
        ("4. УСЛОВИЯ ПОСТАВКИ", [
            "4.1. Адрес и способ поставки Товара согласовывается Сторонами в момент получения заказа.",
            "4.2. При доставке Товара Поставщиком цена формируется с учетом расходов по доставке если иное не оговорено дополнительно.",
            "4.3. В случае доставки Товара силами Поставщика, Покупатель обязан произвести выгрузку Товара в течение 1 (одного) часа с момента прихода транспортного средства на склад Покупателя.",
            "4.4. Разовый заказ не может быть произведен на сумму менее 10 000 (Десяти тысяч) рублей на условиях доставки силами Поставщика, при условии самовывоза Товара Покупателем со склада Поставщика данное условие не действительно.",
            "4.5. Приемка Товара Покупателем по количеству и качеству производится в момент получения Товара от Поставщика при подписании УПД. Подписание УПД свидетельствует об отсутствии претензий по качеству и количеству поставляемого Товара.",
            "4.6. В случае выявления при получении Товара расхождения по количеству или ассортименту, сторонами составляется Акт об установлении расхождений при приемке Товара.",
            "4.7. Получающая сторона должна иметь копию действующей доверенности, подтверждающей право осуществлять приемку Товара и подписания соответствующих товарораспорядительных документов.",
            "4.8. При обнаружении скрытых недостатков Товара Покупатель направляет Поставщику Акт об установленном расхождении. Срок обнаружения скрытых недостатков 3 (три) календарных дня.",
        ]),
        ("5. ГАРАНТИИ И ОТВЕТСТВЕННОСТЬ СТОРОН", [
            "5.1. Ответственность сторон определяется в соответствии с действующим законодательством РФ.",
            "5.2. При несвоевременной оплате Товара Поставщик вправе взыскать с Покупателя неустойку на сумму просроченного платежа в размере 0,5% за каждый календарный день просрочки платежа.",
            "5.3. Убытки, возникшие при транспортировке, в результате ненадлежащего хранения, несоблюдения температурного режима или разгрузки Покупателем поставленной продукции, возмещению Поставщиком не подлежат.",
            "5.4. Поставщик не несет ответственность по обязательствам Покупателя перед третьими лицами.",
            "5.5. Стороны обязуются проводить финансовые сверки взаиморасчетов не реже одного раза в квартал.",
        ]),
        ("6. СРОК ДЕЙСТВИЯ, РАСТОРЖЕНИЕ И ИЗМЕНЕНИЕ ДОГОВОРА", [
            "6.1. Поставщик вправе в одностороннем порядке расторгнуть договор в течение 10 (Десяти) календарных дней с момента направления уведомления о расторжении Договора.",
            "6.2. Договор вступает в силу с момента подписания Сторонами и действует до окончания года следующего за годом подписания договора. При отсутствии возражений сторон за 10 (десять) дней до окончания срока действия договор считается пролонгированным.",
        ]),
        ("7. ИНЫЕ УСЛОВИЯ", [
            "7.1. Все споры разрешаются путем переговоров, при невозможности — в Арбитражном суде города Москвы.",
            "7.2. Сканированные копии настоящего договора обладают полной юридической силой.",
            "7.3. В случае внесения изменений в регистрационные документы Стороны обязуются уведомить об этом в течение 10 (десяти) рабочих дней.",
            "7.4. Все изменения и дополнения оформляются в письменном виде.",
            "7.5. Стороны выражают понимание того, что содержание данного Договора является конфиденциальной информацией.",
            "7.6. Договор составлен в двух подлинных экземплярах, имеющих равную юридическую силу.",
            "7.7. НАЛОГИ И СБОРЫ. Каждая из Сторон несет самостоятельную ответственность за правильность исчисления и своевременность уплаты налогов.",
            "7.8. ИСПОЛНИТЕЛЬНАЯ НАДПИСЬ НОТАРИУСА. Стороны безотзывно уполномочивают любого нотариуса на территории Российской Федерации совершать исполнительную надпись на любом документе, подтверждающем задолженность Покупателя перед Поставщиком.",
        ]),
    ]

    for sec_title, items in sections:
        story.append(Paragraph(sec_title, heading))
        for item in items:
            story.append(Paragraph(item, normal))
        story.append(Spacer(1, 1*mm))

    story.append(Spacer(1, 4*mm))

    # ── Реквизиты сторон ─────────────────────────────────────────────────────
    supplier_lines = [
        [Paragraph("<b>ПОСТАВЩИК:</b>", small_bold), Paragraph("<b>ПОКУПАТЕЛЬ:</b>", small_bold)],
        [Paragraph("<b>АО «ФИШ ТУ БИЗНЕС»</b>", small_bold), Paragraph(f"<b>{data['buyer_name']}</b>", small_bold)],
        [Paragraph("ИНН: 9713025854", small), Paragraph(f"ИНН: {data.get('buyer_inn','')}", small)],
        [Paragraph("КПП: 771301001", small), Paragraph(f"ОГРН: {data.get('buyer_ogrn','')}", small)],
        [Paragraph("ОГРН: 1257700150553", small), Paragraph(f"Юр. адрес: {data.get('buyer_address','')}", small)],
        [Paragraph("127238, г. Москва, вн. тер. г. муниципальный округ Тимирязевский, проезд 3-й Нижнелихоборский, д. 1А, помещ. 1/1", small),
         Paragraph(f"Банк: {data.get('buyer_bank','')}", small)],
        [Paragraph("Банк: ПАО Сбербанк", small), Paragraph(f"р/с {data.get('buyer_rs','')}", small)],
        [Paragraph("р/с 40702810238720001420", small), Paragraph(f"БИК {data.get('buyer_bik','')}", small)],
        [Paragraph("БИК 044525225", small), Paragraph(f"к/с {data.get('buyer_ks','')}", small)],
        [Paragraph("к/с 30101810400000000225", small), Paragraph(f"Тел.: {data.get('buyer_phone','')}", small)],
        [Paragraph("Тел.: +7 800 700 27 03", small), Paragraph(f"E-mail: {data.get('buyer_email','')}", small)],
        [Paragraph("E-mail: F2b@f2b.group", small), Paragraph("", small)],
    ]

    col_w = W / 2
    req_table = Table(supplier_lines, colWidths=[col_w, col_w])
    req_table.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("TOPPADDING", (0,0), (-1,-1), 2),
        ("BOTTOMPADDING", (0,0), (-1,-1), 2),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("LINEBELOW", (0,0), (-1,0), 0.5, colors.grey),
    ]))
    story.append(req_table)
    story.append(Spacer(1, 4*mm))

    # ── Подписи ──────────────────────────────────────────────────────────────
    story.append(Paragraph("Генеральный Директор", small_bold))
    story.append(Spacer(1, 1*mm))

    # Строка с подписями
    sign_supplier_items = [Paragraph("АО «ФИШ ТУ БИЗНЕС»", small)]
    sign_buyer_items = [Paragraph(data.get("buyer_name", ""), small)]

    # Подпись поставщика
    if os.path.exists(SIGN_PATH):
        sign_img = Image(SIGN_PATH, width=35*mm, height=15*mm)
        sign_supplier_items.append(sign_img)
    else:
        sign_supplier_items.append(Paragraph("_____________", small))

    sign_supplier_items.append(Paragraph("/Маланчук А.В./", small_bold))

    # Печать поставщика
    if os.path.exists(STAMP_PATH):
        stamp_img = Image(STAMP_PATH, width=28*mm, height=28*mm)
    else:
        stamp_img = Paragraph("", small)

    sign_buyer_items.append(Paragraph("_____________", small))
    sign_buyer_items.append(Paragraph(f"/{data.get('buyer_director_name','')}/", small_bold))

    sign_data = [
        [
            Table([[x] for x in sign_supplier_items],
                  colWidths=[col_w - 5*mm]),
            stamp_img,
            Table([[x] for x in sign_buyer_items],
                  colWidths=[col_w - 15*mm]),
        ]
    ]

    sign_table = Table(sign_data, colWidths=[col_w - 5*mm, 30*mm, col_w - 10*mm])
    sign_table.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("TOPPADDING", (0,0), (-1,-1), 0),
        ("BOTTOMPADDING", (0,0), (-1,-1), 0),
        ("LEFTPADDING", (0,0), (-1,-1), 2),
    ]))
    story.append(sign_table)

    doc.build(story)
    return buf.getvalue()


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
