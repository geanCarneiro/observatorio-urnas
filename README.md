# observatorio-urnas

Coleta de arquivos públicos do TSE por seção, preservando bytes originais, metadados HTTP e SHA-256. O inventário oficial `cs.jws` permite descobrir as seções por UF; filtros opcionais de UF, município, zona e seção reduzem a abrangência. Os manifestos, BU, RDV e logs continuam sendo processados pelo mesmo coletor por seção.

## Descobrir e filtrar seções

O catálogo estadual é salvo sem alteração em `inventario/catalogs/`, e o inventário filtrado em `inventario/section-inventory.json`. Datas e horas do catálogo (`da`, `ha`) são preservadas como texto bruto. A assinatura JWS é registrada como não verificada.

```sh
# Uma seção, para reproduzir o piloto
python tools/discover_sections.py --uf es --municipio 56251 --zona 0054 --secao 0509 --limit 10

# Todas as seções da zona 0054 de Cariacica
python tools/discover_sections.py --uf es --municipio 56251 --zona 0054 --limit 1000

# Primeiras 100 seções do inventário nacional; aumente o offset para outra página
python tools/discover_sections.py --limit 100 --offset 0
python tools/collect_sections.py --sections inventario/section-inventory.json --data-id 3221 \
  --output dados
```

`--config-id` escolhe o catálogo de seções e `--data-id` escolhe a base dos manifestos por seção. Para as Eleições 2026 observadas aqui, `3220` corresponde ao primeiro turno e `3221` ao segundo; os identificadores e datas da fonte são preservados nos artefatos. O limite do CLI é 5.000 seções; a execução pelo Actions limita cada lote a 100. Para seções específicas, prefira os filtros geográficos, que são mais estáveis do que paginação enquanto o catálogo oficial muda.

## Rodar o piloto de Cariacica

```sh
python3 -m pip install -r requirements.txt
python3 tools/collect_section.py --uf es --municipio 56251 --zona 0054 --secao 0509 --output dados
ACQ=$(find dados/es/56251/0054/0509 -name acquisition.json -type f | sort | tail -1)
python3 tools/extract_section_data.py --acquisition "$ACQ"
```

Os originais e seus JSONs estruturados ficam juntos sob `dados/`, ignorado pelo Git. O JSON do log preserva os timestamps e eventos individuais sem mensagens brutas nem identificadores. Cortes `--cut HH:MM` podem ser repetidos; os horários permanecem na escala bruta até que o fuso seja validado.

## GitHub Actions

Na aba **Actions**, escolha **Coleta por abrangência** e o turno (`1`, `2` ou `ambos`). O catálogo e os dados são descobertos/coletados com os IDs correspondentes. Informe qualquer combinação de UF, município, zona e seção; cada campo vazio amplia o escopo. Por exemplo, apenas UF traz seções de todos os municípios e zonas daquela UF. Todos vazios consultam o inventário nacional. O lote coleta até 10 seções por padrão (ajustável até 100). O workflow publica inventário e dados originais/extraídos juntos num artefato por 90 dias; não publica uma árvore paralela de relatórios. Ele não é disparado automaticamente.

Cada seção fica sob `dados/<uf>/<municipio>/<zona>/<secao>/`. A coleta organiza BU e RDV em `turno-1/` ou `turno-2/`, usando o `idPleito` extraído do BU; o log fica na pasta `logs/` da seção e os eventos têm um JSON extraído ao lado do original. Os JSONs de BU e RDV também ficam ao lado dos arquivos originais. `acquisition.json` preserva URL, hashes, estado e proveniência da aquisição. Um `vota.vsc` listado no manifesto é registrado como não aplicável e não é baixado. O JSON do log preserva os eventos um a um, com timestamp, data, hora e flags por evento para os cortes configurados; não gera agregação ou relatório consolidado.

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

O workflow **Piloto de coleta de seção** continua disponível para repetir uma única seção.

Consulte `docs/` para a auditoria da referência, desenho de escala e evidências/limitações do piloto.
