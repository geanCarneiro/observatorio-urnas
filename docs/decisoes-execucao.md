# Execuções por seção e nacionais

## Abrangência configurável

O catálogo `arquivo-urna/3220/config/{uf}/{uf}-p003220-cs.jws` fornece a hierarquia de municípios, zonas e seções (`abr → mu → zon → sec`). A aplicação o baixa por UF, preserva cada JWS recebido, calcula SHA-256, registra a resposta HTTP e produz um inventário ordenado. O payload do catálogo é decodificado para seleção, mas a assinatura JWS permanece explicitamente não verificada.

## Interface planejada

- Cada campo UF, município, zona e seção é um filtro opcional e cumulativo. Campos em branco ampliam a busca; todos em branco consultam as UFs e o exterior.
- `tools/discover_sections.py` aceita `--config-id`, os quatro filtros, `--offset` e `--limit`; salva os JWS originais e `section-inventory.json` com os parâmetros, hashes, cobertura e diagnósticos.
- `tools/collect_sections.py` aceita o array ou relatório de inventário e encaminha cada seção ao mesmo coletor de seção. `--data-id` seleciona a base independente dos manifestos (`3221` por padrão); o identificador de catálogo é `3220` por padrão.
- Paginação é ordenada por UF/município/zona/seção. Durante a publicação progressiva dos dados pelo TSE, filtros territoriais são preferíveis a offsets, pois a composição do catálogo pode mudar entre execuções. Cada execução registra os metadados e o hash observados.

## GitHub Actions

- `Coleta por abrangência` é manual (`workflow_dispatch`), aceita qualquer combinação dos quatro filtros, enumera seções e executa um lote de até 100 seções. O padrão é 10; nenhum job roda sem acionamento explícito.
- Na interface do Actions, turno, UF, município, zona, seção e limite do lote são parâmetros operacionais. Para o primeiro turno são consultados os catálogos/dados `3220`; para o segundo, `3221`. A pasta final do turno é escolhida após extrair `idPleito` do cabeçalho ASN.1 do BU; o `data_id` solicitado serve à aquisição e é comparado com os metadados encontrados no arquivo.
- `Piloto de coleta de seção` permanece disponível para repetição direta de uma seção.
- Os filtros são cumulativos: somente UF inclui todos os municípios, zonas e seções daquela UF; somente município seleciona esse município em todas as UFs; campos vazios não restringem. Se o resultado ultrapassa o limite, o lote coleta os primeiros itens em ordem estável e o resumo informa que houve truncamento. A interface Action não expõe `offset`; o CLI ainda permite paginação.
- A execução sem filtros descobre o inventário nacional, mas coleta apenas até o limite do lote, nunca o país inteiro numa única execução por padrão. Para cobrir uma área maior que o limite, o operador a divide por filtros territoriais e revê catálogo, duração, tamanho e diagnósticos entre execuções.
- Os arquivos são organizados em `dados/<uf>/<municipio>/<zona>/<secao>/turno-N/runs/<aquisicao>/`, com BU e RDV em diretórios próprios, extração JSON ao lado do original e `acquisition.json` para proveniência. Logs ficam em `dados/<uf>/<municipio>/<zona>/<secao>/logs/`, pois podem conter eventos de mais de um dia/turno. Inventários e coletas preservam parâmetros, URLs, hashes, respostas HTTP e falhas; o artefato tem retenção de 90 dias. O lote não é um checkpoint durável: offsets referem-se ao catálogo obtido naquela execução.
- O processamento preserva RDV original, manifesto e metadados de aquisição. O JSON do RDV fica junto do original e inclui registros individuais, campos estruturados e agregações por cargo. Cada evento do log fica como registro individual com o timestamp bruto; datas/cortes são atributos do evento, sem relatório agregado em árvore separada. Não relacionar registros a eleitores, horários ou logs; não tratar posição como tempo nem como chave compartilhada entre cargos. `vota.vsc` fica fora do escopo, não é baixado e aparece como não aplicável.
- A workflow está habilitada apenas para acionamento manual e limitada a 100 seções por execução. Lotes nacionais maiores dependem de validar o padrão em todas as UFs, custo, armazenamento, assinatura e estabilidade do inventário. Filesystem efêmero do runner não é checkpoint.

## Portões de validação

1. Auditar licença e decisões do projeto de referência; não importar dados derivados.
2. Revalidar o manifesto oficial da seção piloto, interpretar o formato assinado e sua cadeia de confiança, listar todos os hashes/arquivos e baixar BU e log sem substituir bytes originais.
3. Confirmar checksum local, metadados HTTP, armazenamento atômico, retomada/idempotência e parser sem presumir nomes de eventos.
4. Antes de lotes maiores, validar o padrão `cs.jws` em todas as UFs, a completude semântica do inventário, o vínculo entre IDs de catálogo e de dados, assinatura criptográfica e custo real do lote. A configuração do workflow, por si só, não inicia uma execução.

## Limites analíticos

Preservar timestamps brutos; não classificar automaticamente uma urna por um único corte de horário. Correlação não demonstra causalidade, intenção ou irregularidade. Não atribuir voto individual a candidato ou horário. Toda métrica precisa expor cobertura, exclusões, definições e incerteza.
