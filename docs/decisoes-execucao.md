# Execuções por seção e nacionais

## Abrangência configurável

O catálogo `arquivo-urna/3220/config/{uf}/{uf}-p003220-cs.jws` fornece a hierarquia de municípios, zonas e seções (`abr → mu → zon → sec`). A aplicação o baixa por UF, preserva cada JWS recebido, calcula SHA-256, registra a resposta HTTP e produz um inventário ordenado. O payload do catálogo é decodificado para seleção, mas a assinatura JWS permanece explicitamente não verificada.

Os campos de agregação `nsp` e `nsa` são preservados sem alteração em cada linha do inventário. A aplicação deriva e valida os vínculos nos dois sentidos dentro da UF/município/zona: `nsp` aponta para a seção principal e `nsa` lista as seções agregadas. As inconsistências têm códigos estáveis `SECTION_AGGREGATION_*` em `diagnostics`; os dados originais continuam disponíveis no catálogo JWS preservado e nos campos brutos do JSON. Seções agregadas recebem estado `aggregated` e não são enviadas ao coletor de arquivos. Os relatórios separam cadastradas, agregadas, coletáveis, efetivamente coletadas, falhas reais e não processadas; a falta de manifesto de aquisição, isoladamente, significa `not_processed`, não falha HTTP.

## Interface planejada

- Cada campo UF, município, zona e seção é um filtro opcional e cumulativo. Campos em branco ampliam a busca; todos em branco consultam as UFs e o exterior.
- `tools/discover_sections.py` aceita `--config-id`, os quatro filtros, `--offset` e `--limit`; salva os JWS originais e `section-inventory.json` com os parâmetros, hashes, cobertura e diagnósticos.
- `tools/collect_sections.py` aceita o array ou relatório de inventário e encaminha uma faixa de até 100 seções ao mesmo coletor de seção. `--offset` e `--limit` selecionam a faixa; `--data-id` seleciona a base independente dos manifestos (`3221` por padrão); o identificador de catálogo é `3220` por padrão.
- Paginação é ordenada por UF/município/zona/seção. Durante a publicação progressiva dos dados pelo TSE, filtros territoriais são preferíveis a offsets, pois a composição do catálogo pode mudar entre execuções. Cada execução registra os metadados e o hash observados.

## GitHub Actions

- `Coleta por abrangência` é manual (`workflow_dispatch`), aceita qualquer combinação dos quatro filtros e seleciona até 5.000 seções por turno. Divide a seleção em faixas de até 100, executa cada faixa em um job independente e sequencial, e reúne as faixas concluídas em um artefato final. Cada job tem timeout de 180 minutos; nenhum roda sem acionamento explícito.
- Na interface do Actions, turno, UF, município, zona, seção, `offset` e `max_secoes` são parâmetros operacionais. O máximo é por turno; em `ambos`, cada turno recebe sua própria seleção e paginação. Para o primeiro turno são consultados os catálogos/dados `3220`; para o segundo, `3221`. A pasta final do turno é escolhida após extrair `idPleito` do cabeçalho ASN.1 do BU; o `data_id` solicitado serve à aquisição e é comparado com os metadados encontrados no arquivo.
- `Piloto de coleta de seção` permanece disponível para repetição direta de uma seção.
- Os filtros são cumulativos: somente UF inclui todos os municípios, zonas e seções daquela UF; somente município seleciona esse município em todas as UFs; campos vazios não restringem. `offset` e `max_secoes` selecionam uma página ordenada da abrangência resultante.
- A execução sem filtros consulta o inventário nacional, mas coleta somente a página definida por `offset` e `max_secoes` (até 5.000 por turno). Jobs de faixa são sequenciais e têm timeout independente; uma falha em uma faixa não cancela as seguintes, e o artefato final reúne as faixas concluídas. Para continuar além da página, inicie outra execução com o próximo `offset`.
- Os arquivos são organizados em `dados/<uf>/<municipio>/<zona>/<secao>/turno-N/`, com BU e RDV em diretórios próprios, extração JSON ao lado do original e `acquisition.json` para proveniência. Logs ficam em `dados/<uf>/<municipio>/<zona>/<secao>/logs/`, pois podem conter eventos de mais de um dia/turno. Cada pasta mantém a coleta mais recente do turno; os artefatos do Actions guardam o retrato de cada execução por 90 dias. O lote não é um checkpoint durável: offsets referem-se ao catálogo obtido naquela execução.
- O processamento preserva RDV original, manifesto e metadados de aquisição. O JSON do RDV fica junto do original e inclui registros individuais, campos estruturados e agregações por cargo. Cada evento do log fica como registro individual com timestamp bruto; a extração não aplica faixas horárias nem produz contagens consolidadas de eventos. Não relacionar registros a eleitores, horários ou logs; não tratar posição como tempo nem como chave compartilhada entre cargos. `vota.vsc` fica fora do escopo, não é baixado e aparece como não aplicável.
- A workflow está habilitada apenas para acionamento manual e limitada a 5.000 seções selecionadas por turno; a execução pode atravessar vários jobs de faixa. A paginação não é checkpoint durável: outra execução precisa iniciar no próximo `offset`. Filesystem efêmero do runner não é checkpoint.

## Portões de validação

1. Auditar licença e decisões do projeto de referência; não importar dados derivados.
2. Revalidar o manifesto oficial da seção piloto, interpretar o formato assinado e sua cadeia de confiança, listar todos os hashes/arquivos e baixar BU e log sem substituir bytes originais.
3. Confirmar checksum local, metadados HTTP, armazenamento atômico, retomada/idempotência e parser sem presumir nomes de eventos.
4. Antes de lotes maiores, validar o padrão `cs.jws` em todas as UFs, a completude semântica do inventário, o vínculo entre IDs de catálogo e de dados, assinatura criptográfica e custo real do lote. A configuração do workflow, por si só, não inicia uma execução.

## Limites analíticos

Preservar timestamps brutos; não classificar automaticamente uma urna por um único corte de horário. Correlação não demonstra causalidade, intenção ou irregularidade. Não atribuir voto individual a candidato ou horário. Toda métrica precisa expor cobertura, exclusões, definições e incerteza.
