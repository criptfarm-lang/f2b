-- Sink для Wazzup-классификатора → таблица `assortment_requests`.
-- Соответствует плану 2026-05-25-ai-классификатор-wazzup-запросы-номенклатуры.md
-- (раздел "Sink = assortment_requests"). План 2026-05-28-дашборд-закупщика
-- явно говорит: создание этой таблицы — ответственность плана классификатора,
-- не плана дашборда.
--
-- Сырой инбокс автодетектированных запросов клиентов. Если собственник
-- подтверждает (кнопка «→ Заявка закупщику» в TG-алерте) — конвертируется
-- в строку procurement.requests (которую забирает дашборд закупщика).

CREATE TABLE IF NOT EXISTS procurement.assortment_requests (
    id                  BIGSERIAL PRIMARY KEY,
    wazzup_message_id   TEXT NOT NULL UNIQUE,
    chat_id             TEXT NOT NULL,
    contact_name        TEXT,
    manager_name        TEXT,
    raw_text            TEXT NOT NULL,
    species_normalized  TEXT,
    sku_or_description  TEXT,
    urgency             TEXT,
    llm_confidence      NUMERIC(4,3),
    classification_id   BIGINT,  -- FK на wazzup_classifications.id, мягкий

    -- Workflow: pending → converted (в procurement.requests) | rejected
    status              TEXT NOT NULL DEFAULT 'pending',
    converted_request_id BIGINT,  -- FK на procurement.requests.request_id
    status_changed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status_changed_by   TEXT,

    sent_at             TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_assortment_requests_status
    ON procurement.assortment_requests (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_assortment_requests_species
    ON procurement.assortment_requests (species_normalized) WHERE status='pending';
