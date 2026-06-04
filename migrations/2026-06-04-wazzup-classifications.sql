-- Wazzup AI-классификатор: запросы клиентов по номенклатуре.
-- План: F2B второй мозг/plans/2026-05-25-ai-классификатор-wazzup-запросы-номенклатуры.md
-- Эксперимент 2026-06-04: F1=0.97 на 172 примерах (Haiku 4.5 vs Opus 4.7 ground truth).
--
-- Эта миграция — Фаза 1 плана:
-- 1) `wazzup_classifications` — результаты классификации каждого входящего.
-- 2) `wazzup_alerts_sent` — дедуп `(chat_id, species)` × 1h для будущей Фазы 4.
-- 3) `classified_at` в `wazzup_messages` — флаг что worker уже обработал.

-- Безопасно для повторного запуска: все объекты — IF NOT EXISTS.

-- 1. Результаты классификации
CREATE TABLE IF NOT EXISTS wazzup_classifications (
    id              SERIAL PRIMARY KEY,
    message_id      TEXT NOT NULL REFERENCES wazzup_messages(message_id)
                    ON DELETE CASCADE,
    is_nomenclature_request BOOLEAN NOT NULL,
    sku_or_description      TEXT,
    species_normalized      TEXT,    -- лосось/форель/треска/...
    urgency                 TEXT,    -- срочно/уточнение/общий
    confidence              NUMERIC(4,3),  -- 0.000..1.000
    reason                  TEXT,    -- 1-фраза почему
    raw_response            JSONB,   -- сырой ответ Haiku для аудита
    model                   TEXT NOT NULL,   -- claude-haiku-4-5-...
    prompt_version          TEXT NOT NULL,   -- git-sha коммита
    feedback                TEXT,    -- false_positive | confirmed | NULL — для будущего re-train
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(message_id, prompt_version)   -- идемпотентность при retry
);

CREATE INDEX IF NOT EXISTS idx_wazzup_cl_is_request
    ON wazzup_classifications (is_nomenclature_request, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_wazzup_cl_species
    ON wazzup_classifications (species_normalized) WHERE is_nomenclature_request = TRUE;

-- 2. Алерты — дедуп будущей Фазы 4
CREATE TABLE IF NOT EXISTS wazzup_alerts_sent (
    id                   SERIAL PRIMARY KEY,
    chat_id              TEXT NOT NULL,
    species_normalized   TEXT,
    sent_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tg_message_id_owner    BIGINT,   -- для editMessageText дедупа собственнику
    tg_message_id_supplier BIGINT,   -- для закупщика зоны
    tg_message_id_manager  BIGINT    -- для менеджера сделки
);

CREATE INDEX IF NOT EXISTS idx_wazzup_alerts_dedup
    ON wazzup_alerts_sent (chat_id, species_normalized, sent_at DESC);

-- 3. Флаг что worker уже обработал
ALTER TABLE wazzup_messages
    ADD COLUMN IF NOT EXISTS classified_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_wazzup_messages_unclassified
    ON wazzup_messages (sent_at)
    WHERE classified_at IS NULL AND is_outbound = FALSE;
