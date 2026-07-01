-- Результаты сверки «Наш ас-т»: по нажатиям кнопки «🐟 Наш ас-т» в карточке
-- запроса по номенклатуре проверяем, отгрузили ли клиенту запрошенную позицию
-- после нажатия. Пайплайн: assortment_requests(status='our_assortment') →
-- amoCRM компания по имени → ИНН → отгрузки МС этого ИНН с даты нажатия,
-- матч по species. План 2026-07-01-кнопка-наш-ас-т-запрос-номенклатуры.md (Фаза B).
--
-- Считает бот (у него есть клиенты amoCRM/МС), вкладка в дашборде закупок
-- только читает эту таблицу.

CREATE TABLE IF NOT EXISTS procurement.assortment_hit_results (
    id                    BIGSERIAL PRIMARY KEY,
    assortment_request_id BIGINT NOT NULL
        REFERENCES procurement.assortment_requests(id) ON DELETE CASCADE,

    -- Снимок запроса (для отображения без join, но join тоже возможен)
    contact_name          TEXT,
    species_normalized    TEXT,
    sku_or_description     TEXT,
    clicked_at            TIMESTAMPTZ,  -- когда нажали «Наш ас-т» (status_changed_at)

    -- Резолвинг клиента
    amo_company_name      TEXT,
    inn                   TEXT,
    ms_counterparty       TEXT,
    -- matched  = компания найдена по названию + ИНН достан
    -- low      = ИНН достан через контакт (нечёткий путь)
    -- unmatched= ИНН не найден, отгрузку проверить нельзя
    match_confidence      TEXT NOT NULL DEFAULT 'unmatched',

    -- Результат сверки отгрузки
    shipped               BOOLEAN NOT NULL DEFAULT FALSE,
    shipped_qty           NUMERIC,
    shipped_sum           NUMERIC,      -- рубли
    first_shipment_date   DATE,

    -- Окно расчёта
    period_from           DATE NOT NULL,
    period_to             DATE NOT NULL,
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (assortment_request_id, period_from, period_to)
);

CREATE INDEX IF NOT EXISTS idx_assortment_hit_results_period
    ON procurement.assortment_hit_results (period_from, period_to, computed_at DESC);
