SELECT count(*) AS total_registros FROM prata.viagens_validas;

SELECT count(*) AS total_viagens FROM prata.viagens_validas
WHERE data_hora_inicio >= '2022-06-17' AND data_hora_inicio < '2022-06-18'
AND data_hora_fim >= '2022-06-17' AND data_hora_fim < '2022-06-18';

SELECT data_hora_inicio::date AS dia_viagem, data_hora_inicio, data_hora_fim,
distancia_milhas, nome_arquivo, numero_linha
FROM prata.viagens_validas
ORDER BY distancia_milhas DESC, data_hora_inicio, nome_arquivo, numero_linha
LIMIT 5;

WITH estatisticas AS (
 SELECT avg(distancia_milhas) AS media,
 stddev_pop(distancia_milhas) AS desvio_padrao_populacional,
 stddev_samp(distancia_milhas) AS desvio_padrao_amostral,
 min(distancia_milhas) AS minimo,
 max(distancia_milhas) AS maximo,
 percentile_cont(ARRAY[0.25,0.5,0.75]) WITHIN GROUP (ORDER BY distancia_milhas) AS quartis
 FROM prata.viagens_validas
)
SELECT media, desvio_padrao_populacional, desvio_padrao_amostral, minimo, maximo,
quartis[1] AS primeiro_quartil, quartis[2] AS mediana, quartis[3] AS terceiro_quartil
FROM estatisticas;
