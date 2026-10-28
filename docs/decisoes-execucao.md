# Execuções por seção e nacionais

## Abrangência configurável

O catálogo `arquivo-urna/3220/config/{uf}/{uf}-p003220-cs.jws` fornece a hierarquia de municípios, zonas e seções (`abr → mu → zon → sec`). A aplicação o baixa por UF, preserva cada JWS recebido, calcula SHA-256, registra a resposta HTTP e produz um inventário ordenado. O payload do catálogo é decodificado para seleção, mas a assinatura JWS permanece explicitamente não verificada.

Os campos de agregação `nsp` e `nsa` são preservados sem alteração em cada linha do inventário. A aplicação deriva e valida os vínculos nos dois sentidos dentro da UF/município/zona: `nsp` aponta para a seção principal e `nsa` lista as seções agregadas. As inconsistências têm códigos estáveis `SECTION_AGGREGATION_*` em `diagnostics`; os dados originais continuam disponíveis no catálogo JWS preservado e nos campos brutos do JSON. Seções agregadas recebem estado `aggregated` e não são enviadas ao coletor de arquivos. Os relatórios separam cadastradas, agregadas, coletáveis, efetivamente coletadas, falhas reais e não processadas; a falta de manifesto de aquisição, isoladamente, significa `not_processed`, não falha HTTP.

## Interface planejada

- Cada campo UF, município, zona e seção aceita uma lista de códigos separada por vírgulas, com espaços opcionais. Os filtros são cumulativos: município exige UF, zona exige município e seção exige zona; cada lista filha deve existir dentro da seleção dos pais.
- `tools/discover_sections.py` aceita `--config-id`, os quatro filtros em lista, `--offset` e `--limit` opcional. Sem limite, seleciona todas as seções correspondentes; salva os JWS originais e `section-inventory.json` com filtros, hashes, cobertura e diagnósticos. Códigos fora do conjunto pai são rejeitados após a validação do catálogo, antes de criar jobs de coleta.
- `tools/collect_sections.py` aceita o array ou relatório de inventário e encaminha uma faixa de até 1.000 seções ao mesmo coletor de seção. `--offset` e `--limit` selecionam a faixa; `--data-id` seleciona a base independente dos manifestos (`3221` por padrão); o identificador de catálogo é `3220` por padrão.
- Paginação é ordenada por UF/município/zona/seção. Durante a publicação progressiva dos dados pelo TSE, filtros territoriais são preferíveis a offsets, pois a composição do catálogo pode mudar entre execuções. Cada execução registra os metadados e o hash observados.

## GitHub Actions

- `Coleta por abrangência` é manual (`workflow_dispatch`), aceita qualquer combinação dos quatro filtros e não impõe teto total à seleção. `max_secoes` é opcional; vazio seleciona todas as seções dentro dos filtros. O plano imutável divide o inventário em jobs de até 1.000 seções e worker runs de até 100 jobs, cada um com até 10 jobs simultâneos. Cada job tem timeout de 180 minutos. Cada job publica dois artefatos de até 500 seções, sem ZIP nacional consolidado. O workflow principal termina ao disparar os workers; o índice lista os links e lotes de cada worker run.
- Na interface do Actions, operação (`nova` ou `retomar`), turno, UF, município, zona, seção, `offset` e `max_secoes` são parâmetros operacionais. `max_secoes` é aplicado por turno; em `ambos`, cada turno recebe sua própria seleção e paginação. Para o primeiro turno são consultados os catálogos/dados `3220`; para o segundo, `3221`. A pasta final do turno é escolhida após extrair `idPleito` do cabeçalho ASN.1 do BU; o `data_id` solicitado serve à aquisição e é comparado com os metadados encontrados no arquivo.
- `Piloto de coleta de seção` permanece disponível para repetição direta de uma seção.
- Filtros vazios não restringem dentro dos pais selecionados. Somente UF inclui seus municípios, zonas e seções; para usar município, zona ou seção é necessário informar também todos os níveis pais. `offset` e `max_secoes` selecionam uma página ordenada da abrangência resultante.
- A execução sem filtros consulta o inventário nacional; sem `max_secoes`, coleta todas as seções a partir do `offset`. As faixas são distribuídas em worker runs de até 100 jobs. O limite de concorrência do plano do GitHub pode manter jobs excedentes na fila. Os artefatos são publicados por lote e ficam disponíveis por 90 dias.
- Os arquivos são organizados em `dados/<uf>/<municipio>/<zona>/<secao>/turno-N/`, com BU e RDV em diretórios próprios, extração JSON ao lado do original e `acquisition.json` para proveniência. Logs ficam em `dados/<uf>/<municipio>/<zona>/<secao>/logs/`, pois podem conter eventos de mais de um dia/turno. Cada pasta mantém a coleta mais recente do turno; os artefatos do Actions guardam retratos separados de lotes e uma cópia imutável do inventário/plano.
- O processamento preserva RDV original, manifesto e metadados de aquisição. O JSON do RDV fica junto do original e inclui registros individuais, campos estruturados e agregações por cargo. Cada evento do log fica como registro individual com timestamp bruto; a extração não aplica faixas horárias nem produz contagens consolidadas de eventos. Não relacionar registros a eleitores, horários ou logs; não tratar posição como tempo nem como chave compartilhada entre cargos. `vota.vsc` fica fora do escopo, não é baixado e aparece como não aplicável.
- O workflow é manual. Para retomar uma execução, selecione `retomar` e informe o `run_id` original. O índice lista os worker runs; os concluídos com sucesso são ignorados, os ativos não são duplicados e os falhados usam `gh run rerun --failed`, que repete somente os jobs falhados. Se todos tiverem sucesso, a retomada não coleta de novo. Uma nova captura requer uma operação `nova`.

## Portões de validação

1. Auditar licença e decisões do projeto de referência; não importar dados derivados.
2. Revalidar o manifesto oficial da seção piloto, interpretar o formato assinado e sua cadeia de confiança, listar todos os hashes/arquivos e baixar BU e log sem substituir bytes originais.
3. Confirmar checksum local, metadados HTTP, armazenamento atômico, retomada/idempotência e parser sem presumir nomes de eventos.
4. Antes de lotes maiores, validar o padrão `cs.jws` em todas as UFs, a completude semântica do inventário, o vínculo entre IDs de catálogo e de dados, assinatura criptográfica e custo real do lote. A configuração do workflow, por si só, não inicia uma execução.

## Limites analíticos

Preservar timestamps brutos; não classificar automaticamente uma urna por um único corte de horário. Correlação não demonstra causalidade, intenção ou irregularidade. Não atribuir voto individual a candidato ou horário. Toda métrica precisa expor cobertura, exclusões, definições e incerteza.
