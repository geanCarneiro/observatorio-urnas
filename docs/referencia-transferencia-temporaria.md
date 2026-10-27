# Transferência temporária por seção (TSE, 2026)

A referência oficial usa o recurso CKAN `4162f440-1037-4b36-9529-9544120604fa`. A URL do ZIP é descoberta por `resource_show`; ela não é embutida no parser. O ZIP original e seu arquivo `.provenance.json` ficam em `referencias/tse/eleitorado/2026/`, e `reference.json` aponta para a versão corrente. A análise só lê esse arquivo local. Atualização é manual e explícita:

```sh
python tools/temporary_transfer.py --update-reference
```

Cada atualização cria uma versão imutável do ZIP e atualiza o ponteiro. Nenhuma coleta consulta CKAN nem baixa essa base automaticamente.

Antes de implementar o leitor, o cabeçalho do CSV e o `leiame.pdf` no ZIP foram inspecionados. O CSV está em Latin-1, delimitado por `;`, com os campos entre aspas. Os campos usados são `AA_ELEICAO`, `NR_TURNO`, `TP_TTE`, `TP_ABRANGENCIA_TTE`, `SG_UF_DESTINO`, `CD_MUNICIPIO_DESTINO`, `NR_ZONA_DESTINO`, `NR_SECAO_DESTINO` e `QT_ELEITOR`. Códigos de município/zona/seção são normalizados com zeros à esquerda; tipo e abrangência são preservados literalmente e somados por seção de destino, turno, tipo e abrangência. Valores novos de tipo continuam na saída: o arquivo observado contém `Situação de Rua`, modalidade que não consta da enumeração do `leiame.pdf` desta versão.

O dicionário define `Federal` como transferência para outro estado ou país. Para voto em trânsito, define voto apenas para Presidente quando a pessoa está fora da UF de domicílio. A comparação usa todos os registros de abrangência `Federal` como quantidade de referência da seção, mantendo `TP_TTE` separado no detalhamento; isso é uma comparação agregada de compatibilidade, não prova que cada sessão ou cada modalidade federal corresponda a voto exclusivamente presidencial.

Na análise, a métrica conta cada confirmação explícita presidencial em sessões sem confirmação de outro cargo. Pela regra de interpretação configurada na aplicação, a confirmação é a unidade de voto computado; `O voto do eleitor foi computado` encerra a sessão e não acrescenta outro voto. Fim de sessão é verificado separadamente frente ao comparecimento do BU. Sessões suspensas, interrompidas ou com possível retomada mantêm suas confirmações; o vínculo entre sessões permanece indeterminado. Sessões individuais não são ligadas a pessoas ou linhas da base. Os status são:

- `confirmado`: contagem do log igual à quantidade federal da seção; confirmação somente no nível agregado.
- `compatível`: contagem do log menor que a quantidade federal; diferenças podem refletir eleitores que não votaram.
- `divergente`: contagem do log maior que a referência federal observada; o resultado não identifica a causa.
- `dados_insuficientes`: referência inválida/incompleta, turno fora do escopo, métrica de confirmações incompleta, ou confirmação relevante sem associação de sessão. Possíveis retomadas continuam marcadas como indeterminadas, mas não anulam a contagem das confirmações registradas.

Para cada `acquisition.json` passado ao extrator, é produzido na pasta daquele turno um recorte `transferencia-temporaria-elegiveis.csv`, com as linhas destinadas à seção/turno coletados e todas as colunas originais do CSV. Seções fora da aquisição não recebem recorte. O JSON de validação registra também caminho, hash e contagens desse recorte. A métrica de referência presidencial soma todas as modalidades com abrangência `Federal`; os tipos permanecem discriminados, sem confundir o tipo específico `Voto em Trânsito` com o universo de eleitores temporariamente transferidos aptos ao voto presidencial.

O relatório por seção é integrado ao JSON da análise e, no `extract_section_data.py`, também é escrito como `transferencia-temporaria-reconciliacao.json` e `.csv` dentro da pasta do turno. Cada tentativa de cruzamento do segundo turno também gera `transferencia-temporaria-validation.json` no artefato. Esse arquivo registra a identidade da versão e o SHA-256 da base, as seções examinadas, quantidades, diferenças e divergências; o ZIP de referência não é duplicado no artefato. Se a base estiver ausente ou não puder ser lida, o relatório continua sendo produzido com `dados_insuficientes` e a identidade/hash que puderem ser obtidos do manifesto local. Para gerar uma reconciliação avulsa a partir de contagens já fornecidas, sem ler logs, pode-se usar `--reconcile-counts` com um JSON `{ "es/56251/0054/0037": {"count": 13, "event_date": "2026-10-25"} }`.

A contagem de sessões com confirmação apenas presidencial não é informativa para transferência temporária quando o BU do turno contém somente a eleição presidencial: nesse escopo, ela é esperada para todos os votantes. Nessa situação, o cruzamento recebe status `nao_aplicavel`, preserva as contagens e a referência, e não classifica o padrão como sinal de voto em trânsito. A lista de cargos é derivada dos cargos decodificados no BU. Quando outros cargos aparecem no BU, o procedimento anterior de comparação agregada continua disponível, condicionado à completude da métrica do log.
