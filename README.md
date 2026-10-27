# observatorio-urnas

Coleta de arquivos públicos do TSE por seção, preservando bytes originais, metadados HTTP e SHA-256. O inventário oficial `cs.jws` permite descobrir as seções por UF; filtros opcionais de UF, município, zona e seção reduzem a abrangência. Os manifestos, BU, RDV e logs continuam sendo processados pelo mesmo coletor por seção.

## Descobrir e filtrar seções

O catálogo estadual é salvo sem alteração em `inventario/catalogs/`, e o inventário filtrado em `inventario/section-inventory.json`. Datas e horas do catálogo (`da`, `ha`) são preservadas como texto bruto. A assinatura JWS é registrada como não verificada.

Os campos oficiais `nsp` (seção principal referenciada pela seção agregada) e `nsa` (seções agregadas vinculadas à seção principal) também são preservados como recebidos. O inventário acrescenta os relacionamentos normalizados nos dois sentidos e os valida, registrando referências ausentes, vínculos não recíprocos, múltiplos principais e ciclos em `diagnostics`. Seções agregadas são marcadas com `aggregation_role: "aggregated"` e `collection_status: "aggregated"`; o coletor as ignora sem solicitar arquivos individuais. Inventários mostram as contagens cadastradas, agregadas e coletáveis; após a coleta, o workflow registra coletadas, falhas reais e seções ainda não processadas.

A referência oficial do TSE para transferência temporária de eleitorado por seção (2026) está preservada em `referencias/tse/eleitorado/2026/` e integrada às análises por seção/turno. A coleta não atualiza nem baixa essa base; consulte [a documentação da referência e a atualização manual](docs/referencia-transferencia-temporaria.md).

```sh
# Uma seção, para reproduzir o piloto
python tools/discover_sections.py --uf es --municipio 56251 --zona 0054 --secao 0509 --limit 10

# Todas as seções da zona 0054 de Cariacica
python tools/discover_sections.py --uf es --municipio 56251 --zona 0054 --limit 1000

# Descubra até 5.000 seções; depois colete uma faixa de até 100
python tools.discover_sections.py --limit 5000 --offset 0
python tools/collect_sections.py --sections inventario/section-inventory.json --data-id 3221 \
  --offset 0 --limit 100 --output dados
```

`--config-id` escolhe o catálogo de seções e `--data-id` escolhe a base dos manifestos por seção. Para as Eleições 2026 observadas aqui, `3220` corresponde ao primeiro turno e `3221` ao segundo; os identificadores e datas da fonte são preservados nos artefatos. A descoberta seleciona até 5.000 seções por turno; a coleta processa faixas de até 100. Para seções específicas, prefira os filtros geográficos, que são mais estáveis do que paginação enquanto o catálogo oficial muda.

## Rodar o piloto de Cariacica

```sh
python3 -m pip install -r requirements.txt
python3 tools/collect_section.py --uf es --municipio 56251 --zona 0054 --secao 0509 --output dados
ACQ=$(find dados/es/56251/0054/0509 -name acquisition.json -type f | sort | tail -1)
python3 tools/extract_section_data.py --acquisition "$ACQ"
```

Os originais e seus JSONs estruturados ficam juntos sob `dados/`, ignorado pelo Git. O JSON do log preserva os timestamps e eventos individuais sem mensagens brutas nem identificadores. A extração não aplica faixas horárias nem consolida eventos em contagens.

## GitHub Actions

Na aba **Actions**, escolha **Coleta por abrangência** e o turno (`1`, `2` ou `ambos`). Informe qualquer combinação de UF, município, zona e seção; cada campo vazio amplia o escopo. `offset` e `max_secoes` definem a página selecionada, com até 5.000 seções por turno; o workflow divide a página em faixas de até 100 e executa um job independente e sequencial para cada faixa. Cada job tem timeout próprio de 180 minutos, e os arquivos das faixas concluídas são reunidos em um artefato final por 90 dias. O workflow é manual.

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

Consulte `docs/` para a auditoria da referência, desenho de escala e evidências/limitações do piloto.
