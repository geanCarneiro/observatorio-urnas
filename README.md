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

`--config-id` escolhe o catálogo de seções e `--data-id` escolhe a base dos manifestos por seção. Para as Eleições 2026 observadas aqui, `3220` corresponde ao primeiro turno e `3221` ao segundo; os identificadores e datas da fonte são preservados nos artefatos. O limite do CLI é 5.000 seções; a execução pelo Actions limita cada lote a 100. Para seções específicas, prefira os filtros geográficos, que são mais estáveis do que paginação enquanto o catálogo oficial muda.

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

Na aba **Actions**, escolha **Coleta por abrangência** e o turno (`1`, `2` ou `ambos`). O catálogo e os dados são descobertos/coletados com os IDs correspondentes, e cada turno fica em diretórios e relatórios próprios. Informe qualquer combinação de UF, município, zona e seção; cada campo vazio amplia o escopo. Por exemplo, apenas UF traz seções de todos os municípios e zonas daquela UF. Todos vazios consultam o inventário nacional. O lote coleta até 10 seções por padrão (ajustável até 100); se houver mais correspondências, o resumo avisa. O workflow publica catálogos originais, inventário, aquisições e relatórios juntos num artefato por 90 dias. Ele não é disparado automaticamente.

O manifesto pode listar `vota.vsc` como `tp=vota`. É um pacote de assinaturas de software/hardware e declarações de hashes de arquivos assinados da urna, incluindo BU, RDV e log; não é um arquivo de votos. A aplicação preserva os bytes e os hashes locais, mas ainda não interpreta nem verifica as assinaturas nele contidas. O relatório marca essa etapa como não realizada. Os agregados de log agora são particionados pela data registrada em cada evento; no material observado, o log do segundo turno contém datas dos dois turnos e pode ser comparado com cada BU pela data de emissão.

O workflow **Piloto de coleta de seção** continua disponível para repetir uma única seção.

Consulte `docs/` para a auditoria da referência, desenho de escala e evidências/limitações do piloto.
