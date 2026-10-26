# observatorio-urnas

Coleta pontual de arquivos públicos do TSE por seção, preservando bytes originais, metadados HTTP e SHA-256. O piloto atual aceita UF, município, zona e seção; a expansão nacional depende da validação dos formatos e do dimensionamento do armazenamento/checkpoints.

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

Na aba **Actions**, escolha **Piloto de coleta de seção** → **Run workflow**. Os campos já vêm preenchidos com Cariacica/ES, município 56251, zona 0054, seção 0509. Edite os cortes HH:MM se necessário. O workflow baixa manifesto, BU e log, gera o relatório e o publica como artefato por 30 dias. A coleta nacional não está habilitada neste workflow.

Consulte `docs/` para a auditoria da referência, desenho de escala e evidências/limitações do piloto.
