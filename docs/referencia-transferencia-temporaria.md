# Transferência temporária por seção (TSE, 2026)

A referência oficial usa o recurso CKAN `4162f440-1037-4b36-9529-9544120604fa`. A URL do ZIP é descoberta por `resource_show`; ela não é embutida no parser. O ZIP original e seu arquivo `.provenance.json` ficam em `referencias/tse/eleitorado/2026/`, e `reference.json` aponta para a versão corrente. A análise só lê esse arquivo local. Atualização é manual e explícita:

```sh
python tools/temporary_transfer.py --update-reference
```

Cada atualização cria uma versão imutável do ZIP e atualiza o ponteiro. Nenhuma coleta consulta CKAN nem baixa essa base automaticamente.

Antes de implementar o leitor, o cabeçalho do CSV e o `leiame.pdf` no ZIP foram inspecionados. O CSV está em Latin-1, delimitado por `;`, com os campos entre aspas. Os campos usados são `AA_ELEICAO`, `NR_TURNO`, `TP_TTE`, `TP_ABRANGENCIA_TTE`, `SG_UF_DESTINO`, `CD_MUNICIPIO_DESTINO`, `NR_ZONA_DESTINO`, `NR_SECAO_DESTINO` e `QT_ELEITOR`. Códigos de município/zona/seção são normalizados com zeros à esquerda; tipo e abrangência são preservados literalmente e somados por seção de destino, turno, tipo e abrangência. Valores novos de tipo continuam na saída: o arquivo observado contém `Situação de Rua`, modalidade que não consta da enumeração do `leiame.pdf` desta versão.

O dicionário define `Federal` como transferência para outro estado ou país. Para voto em trânsito, define voto apenas para Presidente quando a pessoa está fora da UF de domicílio. A comparação usa todos os registros de abrangência `Federal` como quantidade de referência da seção, mantendo `TP_TTE` separado no detalhamento; isso é uma comparação agregada de compatibilidade, não prova que cada sessão ou cada modalidade federal corresponda a voto exclusivamente presidencial.

Na análise, a métrica do log exige sessão reconstruída com habilitação, confirmação presidencial e evento explícito posterior de voto computado, sem confirmação de governador nem evento de suspensão, interrupção ou falha. Sessões isoladas não são ligadas a pessoas ou linhas da base. Os status são:

- `confirmado`: contagem do log igual à quantidade federal da seção; confirmação somente no nível agregado.
- `compatível`: contagem do log menor que a quantidade federal; diferenças podem refletir eleitores que não votaram.
- `divergente`: contagem do log maior que a referência federal observada; o resultado não identifica a causa.
- `dados_insuficientes`: referência inválida/incompleta, turno fora do escopo, ou métrica de log ausente/ambígua.

O relatório por seção é integrado ao JSON da análise e, no `extract_section_data.py`, também é escrito como `transferencia-temporaria-reconciliacao.json` e `.csv` dentro da pasta do turno. Para gerar uma reconciliação avulsa a partir de contagens já fornecidas, sem ler logs, pode-se usar `--reconcile-counts` com um JSON `{ "es/56251/0054/0037": {"count": 13, "event_date": "2026-10-25"} }`.
