SELECT 
	id,
	data_referencia,
	tipo,
	zona,
	localizacao,
	area,
	quartos,
	banheiros,
	NTILE(4) OVER(ORDER BY preco) AS quartil_preco,
	condo,
	preco,
	ROUND((preco/area), 2) AS preco_m2,
    CURRENT_TIMESTAMP AS ingestion_timestamp
FROM silver_imoveis