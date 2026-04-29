CREATE TABLE IF NOT EXISTS public.etl_runs (
    id BIGSERIAL PRIMARY KEY,
    process_name TEXT NOT NULL,
    report_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    data_start DATE,
    data_end DATE,
    rows_downloaded INTEGER,
    rows_loaded INTEGER,
    source_file TEXT,
    message TEXT
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_report_status_finished_at
    ON public.etl_runs (report_name, status, finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_runs_process_started_at
    ON public.etl_runs (process_name, started_at DESC);

COMMENT ON TABLE public.etl_runs IS
'Auditoria de execucoes do pipeline Transleg. As tabelas de dominio sao criadas automaticamente a partir do catalogo de relatorios.';

