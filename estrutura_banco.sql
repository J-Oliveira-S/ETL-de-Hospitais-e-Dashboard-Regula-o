CREATE TABLE IF NOT EXISTS public.fila_regulacao (
    id bigserial PRIMARY KEY,
    id_paciente integer NOT NULL,
    nome_anonimo text,
    gravidade text,
    procedimento_solicitado text,
    unidade_origem text,
    data_solicitacao timestamp without time zone
);
CREATE TABLE unidades_saude (
    objectid INTEGER,
    globalid TEXT,
    cnes TEXT PRIMARY KEY,
    nome_unidade TEXT,
    tipo TEXT,
    tipo_abc TEXT,
    endereco TEXT,
    bairro TEXT,
    municipio TEXT DEFAULT 'Rio de Janeiro',
    cap TEXT,
    equipes TEXT,
    telefone TEXT,
    email TEXT,
    horario_semana TEXT,
    horario_sabado TEXT,
    data_inauguracao DATE,
    ativo BOOLEAN,
    latitude FLOAT,
    longitude FLOAT
);

SELECT tipo, count(*) FROM unidades_saude GROUP BY tipo ORDER BY count(*) DESC; 
