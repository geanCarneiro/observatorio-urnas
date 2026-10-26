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
  --output dados --report reports/coleta-lote.json
```

`--config-id` escolhe o catálogo de seções (`3220` por padrão); `--data-id` escolhe a base dos manifestos por seção (`3221` por padrão). São identificadores separados. O limite do CLI é 5.000 seções; a execução pelo Actions limita cada lote a 100. Para seções específicas, prefira os filtros geográficos, que são mais estáveis do que paginação enquanto o catálogo oficial muda.

## Rodar o piloto de Cariacica

```sh
python3 -m pip install -r requirements.txt
python3 tools/collect_section.py --uf es --municipio 56251 --zona 0054 --secao 0509 --output dados
RUN=$(find dados/es/56251/0054/0509/runs -mindepth 1 -maxdepth 1 -type d | sort | tail -1)
python3 tools/analyze_section.py --log "$(find "$RUN" -name '*-log.jez' -print -quit)" \
  --bu "$(find "$RUN" -name '*-bu.dat' -print -quit)" \
  --collection "$RUN/collection.json" --output reports/piloto-secao-v2.json
```

Os dados brutos ficam em `dados/`, ignorado pelo Git. O relatório contém timestamps individuais e resumos de uma lista fechada de eventos, sem mensagens brutas nem identificadores do log. Cortes `--cut HH:MM` podem ser repetidos. Os horários permanecem na escala bruta até que o fuso seja validado.

## GitHub Actions

Na aba **Actions**, escolha **Coleta por abrangência**. Informe qualquer combinação de UF, município, zona e seção; cada campo vazio amplia o escopo. Por exemplo, apenas UF traz seções de todos os municípios e zonas daquela UF. Todos vazios consultam o inventário nacional. O lote coleta até 10 seções por padrão (ajustável até 100); se houver mais correspondências, o resumo avisa. O workflow publica catálogos originais, inventário, aquisições e relatórios juntos num artefato por 90 dias. Ele não é disparado automaticamente.

O workflow **Piloto de coleta de seção** continua disponível para repetir uma única seção.

Consulte `docs/` para a auditoria da referência, desenho de escala e evidências/limitações do piloto.
