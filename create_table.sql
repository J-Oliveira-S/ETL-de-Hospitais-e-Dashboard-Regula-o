-- SQL para criar a tabela `fila_regulacao` no Supabase (Postgres)
CREATE TABLE IF NOT EXISTS public.fila_regulacao (
    id bigserial PRIMARY KEY,
    id_paciente integer NOT NULL,
    nome_anonimo text,
    gravidade text,
    procedimento_solicitado text,
    unidade_origem text,
    data_solicitacao timestamp without time zone
);
