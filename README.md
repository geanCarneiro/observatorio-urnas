# observatorio-urnas

Coleta de arquivos públicos do TSE por seção, preservando bytes originais, metadados HTTP e SHA-256. O inventário oficial `cs.jws` permite descobrir as seções por UF; filtros opcionais de UF, município, zona e seção reduzem a abrangência. Os manifestos, BU, RDV e logs continuam sendo processados pelo mesmo coletor por seção.

## Descobrir e filtrar seções

O catálogo estadual é salvo sem alteração em `inventario/catalogs/`, e o inventário filtrado em `inventario/section-inventory.json`. Catálogos de UFs diferentes são consultados em paralelo, com limite de 15 requisições, e o andamento mostra quantos já foram processados. Nos jobs locais da UI/CLI, os bytes e validadores HTTP ficam em `.estado-local/catalog-cache/`; prévias seguintes revalidam `ETag`/`Last-Modified` e reutilizam o conteúdo quando o TSE responde `304`. Catálogos de inventários locais anteriores são aproveitados ao inicializar esse cache. Datas e horas do catálogo (`da`, `ha`) são preservadas como texto bruto. A assinatura JWS é registrada como não verificada.

Se Windows ou o ambiente de execução bloquear conexões de saída (`WinError 10013`), a descoberta interrompe as outras UFs sem repetir a mesma falha e informa que a UI/CLI precisa ser executada em um ambiente com acesso externo à rede.

Os campos oficiais `nsp` (seção principal referenciada pela seção agregada) e `nsa` (seções agregadas vinculadas à seção principal) também são preservados como recebidos. O inventário acrescenta os relacionamentos normalizados nos dois sentidos e os valida, registrando referências ausentes, vínculos não recíprocos, múltiplos principais e ciclos em `diagnostics`. Seções agregadas são marcadas com `aggregation_role: "aggregated"` e `collection_status: "aggregated"`; o coletor as ignora sem solicitar arquivos individuais. Inventários mostram as contagens cadastradas, agregadas e coletáveis; após a coleta, o workflow registra coletadas, falhas reais e seções ainda não processadas.

A referência oficial do TSE para transferência temporária de eleitorado por seção (2026) está preservada em `referencias/tse/eleitorado/2026/` e integrada às análises por seção/turno. A coleta não atualiza nem baixa essa base; consulte [a documentação da referência e a atualização manual](docs/referencia-transferencia-temporaria.md).

```sh
# Uma seção, para reproduzir o piloto
python tools/discover_sections.py --uf es --municipio 56251 --zona 0054 --secao 0509 --limit 10

# Todas as seções da zona 0054 de Cariacica
python tools/discover_sections.py --uf es --municipio 56251 --zona 0054 --limit 1000

# Selecionar várias seções em zonas e municípios dentro das UFs indicadas
python tools/discover_sections.py --uf "es, mg" --municipio "56251, 41009" \
  --zona "0054, 0050" --secao "0509, 0224" --limit 100

# Descubra todo o inventário correspondente; `--limit` é opcional
python tools.discover_sections.py --offset 0
python tools/collect_sections.py --sections inventario/section-inventory.json --data-id 3221 \
  --offset 0 --limit 1000 --output dados
```

`--config-id` escolhe o catálogo de seções e `--data-id` escolhe a base dos manifestos por seção. Para as Eleições 2026 observadas aqui, `3220` corresponde ao primeiro turno e `3221` ao segundo; os identificadores e datas da fonte são preservados nos artefatos. Sem `--limit`, a descoberta seleciona todas as seções correspondentes. O coletor em lote aceita até 1.000 seções por job. Para seções específicas, prefira os filtros geográficos, que são mais estáveis do que paginação enquanto o catálogo oficial muda.

## Rodar o piloto de Cariacica

```sh
python -m pip install -r requirements.txt
python tools/local_jobs.py collect --turn 2 --uf es --municipio 56251 --zona 0054 --secao 0509
```

O job local verifica se a aquisição já está completa e, se necessário, coleta os arquivos ausentes e processa a extração. Os originais e seus JSONs estruturados ficam juntos sob `dados/`, ignorado pelo Git. O JSON do log preserva os timestamps e eventos individuais sem mensagens brutas nem identificadores. A extração não aplica faixas horárias nem consolida eventos em contagens.

Para processar o conjunto selecionado por remessas, use **Coletar e conciliar conjunto completo** na UI ou `python tools/local_jobs.py batch-process --turn ambos --uf "al, ba, ce, ma, pb, pe, pi, rn, se" --workers 30 --confirmar-processamento`. Um job novo pode reutilizar resumos de seção compatíveis do job por remessas mais recente, mantidos fora de `dados/`; os dados brutos são temporários e removidos por seção após consolidação e validação seguras. Seções com consolidação confirmada e resumo íntegro não são auditadas novamente quando não há estado físico incerto. Se houver dados físicos pendentes, falha ou interrupção com estado incerto, a seção é verificada; arquivos ausentes ou divergentes levam à nova coleta e reconciliação daquela seção. Seções sem dados físicos nem tentativa incerta seguem diretamente para coleta. O marcador de consolidação e o hash ficam no SQLite de `.estado-local/runs/<run_id>/`; os resumos finais ficam em `consolidados/<run_id>/`. O resumo e os resultados da auditoria direcionada ficam em `.estado-local/runs/<run_id>/audit-summary.json` e `audit-results.jsonl`. Os checkpoints usados internamente para alimentar a fila não aparecem como remessas na UI. A coleta e reconciliação usam os mesmos workers em uma fila contínua; cada seção é atendida por uma etapa de cada vez. Após salvar os consolidados e preservar evidências de anomalias ou processamento incompleto, os dados brutos são limpos por seção. O resultado final fica separado por estado em `consolidados/<run_id>/<UF>/<município>/<zona>.csv`; a análise é determinística e não grava JSONs detalhados por arquivo.

## GitHub Actions

Na aba **Actions**, escolha **Coleta por abrangência** e a operação (`nova` ou `retomar`). Em uma nova coleta, escolha o turno (`1`, `2` ou `ambos`). UF, município, zona e seção aceitam listas de códigos separadas por vírgula, com espaços opcionais. Os filtros são cumulativos e obedecem à hierarquia: município exige UF, zona exige município, e seção exige zona. Cada código filho precisa existir dentro do conjunto dos pais escolhidos; a validação ocorre durante a descoberta, antes de gerar jobs de coleta. Campos vazios ampliam o escopo dentro do filtro pai. `offset` seleciona o início da página; `max_secoes` é opcional e, quando vazio, coleta toda a abrangência filtrada por turno.

O workflow principal preserva o inventário e o plano em `coleta-plano-<run_id>` e registra os links dos workers em `coleta-index-<run_id>`. A coleta é executada nos worker runs: cada job processa até 1.000 seções, worker runs têm até 100 jobs e executam até 10 em paralelo. O GitHub mantém jobs excedentes na fila conforme a capacidade de runners da conta. Cada job publica artefatos separados de até 500 seções, mantidos por 90 dias; não é criado um ZIP nacional consolidado. O workflow principal termina após disparar os workers; acompanhe a conclusão e falhas nos worker runs listados no índice. Cada job tem timeout de 180 minutos.

Para retomar, selecione `retomar` e informe em `run_id_origem` o ID da execução original. O workflow consulta os worker runs registrados no índice daquela execução: jobs concluídos com sucesso são ignorados, jobs ainda ativos não são duplicados e somente jobs falhados são reexecutados. Se tudo já terminou com sucesso, a retomada não baixa os dados novamente. Para obter uma captura nova, inicie uma operação `nova`.

Cada seção fica sob `dados/<uf>/<municipio>/<zona>/<secao>/`. A coleta organiza BU e RDV em `turno-1/` ou `turno-2/`, usando o `idPleito` extraído do BU; o log fica na pasta `logs/` da seção e os eventos têm um JSON extraído ao lado do original. Os JSONs de BU e RDV também ficam ao lado dos arquivos originais. `acquisition.json` preserva URL, hashes, estado e proveniência da aquisição. Um `vota.vsc` listado no manifesto é registrado como não aplicável e não é baixado. O JSON do log preserva cada evento e seu timestamp, sem faixas horárias, contagens consolidadas ou relatório paralelo.

```text
dados/es/56251/0054/0509/
├── turno-1/
│   ├── acquisition.json
│   ├── bu/<arquivo-bu>.dat[.json]
│   └── rdv/<arquivo-rdv>.dat[.json]
├── turno-2/...
└── logs/<arquivo-log>.jez[.json]
```

Cada pasta de turno mantém a aquisição mais recente daquele turno. Uma nova coleta no mesmo diretório atualiza arquivos com o mesmo nome oficial; o `acquisition.json` registra a proveniência da versão presente. O Actions publica um artefato imutável por execução.

O workflow **Piloto de coleta de seção** coleta os dois turnos de uma única seção em uma execução.

## Execução local

Instale as dependências uma vez no ambiente Python usado na máquina:

```powershell
python -m pip install -r requirements.txt
```

O comando local mantém a CLI disponível para automação e uso no terminal:

```powershell
# Coleta incremental do segundo turno; filtros vazios significam abrangência nacional
python tools/local_jobs.py collect --turn 2 --uf es --municipio 56251 --zona 0054

# Buscar novamente os brutos para as seções selecionadas e substituir arquivos de mesmo nome
python tools/local_jobs.py collect --turn 2 --uf es --municipio 56251 --zona 0054 --refresh

# Reprocessar os dados já baixados, sem coletar arquivos brutos
python tools/local_jobs.py reprocess --workers 30

# Listar e retomar uma execução local interrompida ou incompleta
python tools/local_jobs.py list-runs
python tools/local_jobs.py status
python tools/local_jobs.py watch
python tools/local_jobs.py stop
python tools/local_jobs.py resume --run-id <id-da-execução>
```

Jobs iniciados pela UI ou pelos comandos de execução da CLI rodam em um processo worker destacado e continuam se a interface que os iniciou for fechada. UI e CLI controlam a execução persistida em `.estado-local/runs/`; `status` mostra o estado atual, `watch` segue as atividades e `stop` solicita a parada segura do job ativo. Interromper `watch` fecha apenas o acompanhamento; use `stop` para solicitar que o job pare.

Durante a extração, a CLI emite eventos JSON `job_snapshot` (até uma atualização por segundo) com os workers ativos, seu percentual, a quantidade aguardando e as próximas seções da fila.

Por padrão, a coleta incremental pula aquisições completas da mesma seção e turno. Em aquisições incompletas, consulta o manifesto atual e reutiliza os arquivos locais que continuam listados com o mesmo hash e tamanho; baixa ou atualiza os demais. `--refresh` ignora essa reutilização e baixa novamente os arquivos listados. Os arquivos são publicados atomicamente, substituindo nomes iguais sem remover arquivos antigos com outros nomes.

Para disparar e acompanhar os mesmos jobs por uma UI local no navegador, dê duplo clique em `abrir-observatorio.bat`. O launcher abre o navegador e mantém um terminal aberto para acompanhar o job; `Ctrl+C` encerra o servidor.

Também é possível iniciar pelo terminal:

```powershell
python tools/local_ui.py
```

A UI permite pré-visualizar o escopo, coletar incrementalmente, atualizar os brutos, reprocessar localmente e retomar uma execução. Ela usa apenas a biblioteca padrão do Python, abre em `127.0.0.1` e recebe atualizações em tempo real por SSE; comandos continuam sendo enviados por HTTP. A descoberta consulta até 15 catálogos de UF em paralelo. A quantidade de workers é configurável na UI e pela opção `--workers` na CLI; o padrão é 15. Cada worker de execução é um processo Python independente, reutilizado enquanto o job está ativo; o processo coordenador atribui tarefas e é o único responsável pelo SQLite, checkpoints e consolidação final. No Windows, `ProcessPoolExecutor` aceita até 61 processos worker. No processamento por remessas, os workers auditam somente sessões pendentes que precisam de verificação; resultados consolidados compatíveis são reutilizados e a mesma quantidade de processos atende coleta e conciliação numa fila contínua. O PID de cada processo aparece no cartão da tarefa ativa. A atividade recente é restaurada de `events.jsonl` após reconexão da UI. O painel mantém um cartão por processo configurado; os ociosos indicam “Parado”. A mesma seção geográfica não roda em dois processos worker simultaneamente porque os logs dos turnos compartilham a pasta `logs/` da seção. **Parar após os processos ativos terminarem** salva o andamento para retomada. Os detalhes de cada execução incluem parâmetros, plano, contagens, eventos recentes e registros de seção paginados; os parâmetros podem ser copiados ou usados para iniciar uma nova execução após uma prévia atualizada. O reprocessamento usa os arquivos sob `dados/` e as referências locais do repositório, sem baixar novos dados brutos, e usa a mesma quantidade configurável de processos.

### Dashboard nativo em C#

O dashboard desktop fica separado em `frontend/ObservatorioUrnasDashboard/`. Execute `abrir-dashboard-nativo.bat` na raiz para compilar a versão atual em Release e abrir a janela. O cabeçalho mostra somente o progresso geral. Os cards distinguem workers executando, aguardando tarefa e job pausado; durante a espera, preservam a última seção e etapa, resultado e tempo desde a conclusão. A janela distribui os cards com largura mínima e expande igualmente os cards da última linha. O dashboard mantém na home apenas o job atual/mais recente; a janela de histórico mostra detalhes e permite copiar parâmetros para um novo job. A janela Novo job reúne coleta incremental/atualização bruta, processamento completo e reprocessamento local, com prévia antes de iniciar e confirmação do processamento por remessas, que reutiliza resultados consolidados compatíveis e limpa os dados brutos apenas por seção após consolidação segura. A home permite retomar o job mais recente, solicitar parada segura e abrir a interface web completa. O servidor Python local é iniciado em segundo plano; fechar o dashboard encerra apenas esse servidor de interface. O comando `dotnet run --project frontend/ObservatorioUrnasDashboard/ObservatorioUrnasDashboard.csproj` continua disponível para execução pelo terminal.

Os inventários, planos, checkpoints SQLite, eventos (`events.jsonl`) e arquivos temporários dos jobs ficam em `.estado-local/runs/<id>/`, separado de `dados/` e ignorado pelo Git. O cache revalidável dos catálogos fica em `.estado-local/catalog-cache/`. `dados/` contém apenas as árvores de amostras, arquivos brutos e saídas derivadas. Uma retomada usa o estado da mesma execução; uma nova coleta incremental também aproveita aquisições completas já presentes em `dados/`.

Consulte `docs/` para a auditoria da referência, desenho de escala e evidências/limitações do piloto.
