WITH clean AS (
    SELECT 
        md5(
            COALESCE(origem, '') || 
            COALESCE(tipo, '') || 
            COALESCE(localizacao, '') || 
            COALESCE(area, '') || 
            COALESCE(quartos, '') || 
            COALESCE(banheiros, '') || 
            COALESCE(CAST(preco AS TEXT), '')
        ) AS id,
        STRFTIME(CAST(ingestion_timestamp AS TIMESTAMP), '%m-%Y') AS data_referencia,
        origem,
        tipo,
        zona,
        localizacao,
        COALESCE(TRY_CAST(area AS INTEGER), 0) AS area,
        COALESCE(TRY_CAST(quartos AS INTEGER), 0) AS quartos,
        COALESCE(TRY_CAST(banheiros AS INTEGER), 0) AS banheiros,
        COALESCE(TRY_CAST(vagas AS INTEGER), 0) AS vagas,
        COALESCE(TRY_CAST(condo AS REAL), 0.0) AS condo,
        COALESCE(TRY_CAST(preco AS REAL), 0.0) AS preco,
        ingestion_timestamp
    FROM bronze_imoveis
    WHERE
        TRY_CAST(banheiros AS INTEGER) > 0
        AND TRY_CAST(preco AS REAL) > 0
		AND TRY_CAST(quartos AS INTEGER) > 0
),
deduplication AS (
	SELECT 
		*, 
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_timestamp DESC) AS row_num 
	FROM clean
)
SELECT 
    id,
    data_referencia,
    origem,
    tipo,
    zona,
    localizacao,
    area,
    quartos,
    banheiros,
    vagas,
    condo,
    preco,
    CURRENT_TIMESTAMP AS ingestion_timestamp
FROM deduplication
WHERE row_num = 1;
