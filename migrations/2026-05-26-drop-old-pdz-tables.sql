-- Миграция-зачистка старых таблиц ПДЗ-механики.
--
-- Контекст: с 2026-05-20 PDZ-автоматика переведена с ручных отчётов менеджеров
-- в личку «Эф» на структурное поле «Новая дата оплаты» в МойСкладе. Старые
-- таблицы pdz_results, pdz_comments, debtors больше не пишутся и не читаются
-- (см. план plans/2026-05-20-пдз-автоматика.md, Фаза 1 — чистка кода, коммит
-- 54bd0c6). По плану Фазы 7 эти таблицы дропаются после успешного теста новой
-- механики менеджерами.
--
-- ВНИМАНИЕ: операция деструктивная. Перед запуском убедись:
--   1. Новая механика стабильно работает (минимум 5 успешных боевых рассылок 13:00).
--   2. Никто из менеджеров/собственника не ссылается на эти таблицы в отчётах.
--   3. Если есть желание сохранить историю — сделать `pg_dump` отдельной операцией:
--        pg_dump --table=pdz_results --table=pdz_comments --table=debtors \
--          "postgresql://Victor:Archor973@f2b-postgres-victor03.db-msk0.amvera.tech:5432/f2bbot" \
--          > pdz_archive_2026_05_26.sql
--
-- Запуск (используется libpq 16 из-за ALPN-нюанса Amvera Postgres):
--   /opt/homebrew/opt/postgresql@16/bin/psql \
--     "postgresql://Victor:Archor973@f2b-postgres-victor03.db-msk0.amvera.tech:5432/f2bbot?sslmode=require" \
--     -f migrations/2026-05-26-drop-old-pdz-tables.sql

BEGIN;

DROP TABLE IF EXISTS pdz_results  CASCADE;
DROP TABLE IF EXISTS pdz_comments CASCADE;
DROP TABLE IF EXISTS debtors      CASCADE;

-- После DROP — пометить в bot_settings, чтобы init_db() при следующем
-- старте бота не пересоздал таблицы заново.
INSERT INTO bot_settings (key, value)
VALUES ('migration_2026_05_26_drop_old_pdz_tables', NOW()::text)
ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value;

COMMIT;

-- После применения этой миграции — выпилить из database.py:
--   - CREATE TABLE IF NOT EXISTS debtors      (строка ~108)
--   - CREATE TABLE IF NOT EXISTS pdz_comments (строка ~134)
--   - CREATE TABLE IF NOT EXISTS pdz_results  (строка ~349)
-- иначе при следующем рестарте бот их пересоздаст пустыми.
