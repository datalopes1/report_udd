SELECT 
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
FROM raw_imoveis;