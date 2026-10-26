# Comparação dos artefatos do piloto

Seção: ES / município 56251 / zona 0054 / seção 0509. A coleta foi repetida em 26/10/2026. O manifesto, o log e o BU vieram com os mesmos SHA-256 nas duas aquisições, portanto as diferenças abaixo são de processamento/formato do relatório, não de bytes de origem.

| Métrica | v1 | v2 |
|---|---:|---:|
| Eventos relevantes no log | 2.434 agregados | 2.434 eventos preservados individualmente |
| Votos computados em 04/10/2026 | 296 | 296 |
| Votos computados em 25/10/2026 | 311 | 311 |
| Última habilitação em 25/10 (relógio bruto) | ausente | 16:50:24 |
| Último voto computado em 25/10 (relógio bruto) | ausente | 16:50:39 |
| Comando / início / conclusão do encerramento | nomes agregados | linhas, nomes distintos e timestamps completos |
| Cortes de atividade | lista fixa, sem configuração pela execução | `--cut HH:MM` repetível e input de workflow |
| Integridade dos arquivos | hashes da coleta | hashes recalculados e comparados com o registro da coleta; CRC do ZIP válido |
| BU e relógio | sem validação | identidade do envelope bate com a seção; encerramento do BU e conclusão do log coincidem em 17:00:28 |

Limites que permanecem: o timestamp não declara fuso; o acordo entre os dois relógios da urna não define UTC/Brasília. O evento chamado “Inicio do Encerramento” aparece no arquivo depois da conclusão, embora seus timestamps permaneçam em ordem crescente. Não presumimos equivalência semântica. A verificação interna do BU falha com o schema disponível, então contagens BU e divergências seguem `null`. O manifesto mostra uma entrada de hash, evidência insuficiente para excluir substituições anteriores. A assinatura JWS não foi verificada.

O artefato detalhado v2 contém timestamps e categorias permitidas para cada evento, com número da linha e SHA-256 do arquivo de origem. Mensagens brutas e identificadores individuais são excluídos.
