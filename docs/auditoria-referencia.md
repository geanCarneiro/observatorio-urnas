# Auditoria inicial de `urnas-tardias`

Data da inspeção: 2026-10-26. Referência inspecionada: `Jonnes-Santos/urnas-tardias`, commit `b650fd02d89e691b2eef1a0a5a67fbe17da6619c`.

- O commit inclui a implementação do dashboard e o pipeline de coleta/análise herdado da referência `apolinario/urnas-tardias`.
- O checkout inspecionado não contém arquivo `LICENSE`. Sem licença explícita, não adaptar nem copiar código do repositório. Usar apenas conceitos gerais e escrever implementação independente, salvo esclarecimento posterior do titular.
- O coletor observado acessa `.../arquivo-urna/3221`, busca arquivo auxiliar JSON (sufixo `.json` no código), seleciona apenas o último item de `hashes[]` e o primeiro log encontrado. Isso diverge do formato `.jws` e da necessidade de preservar e inspecionar todos os itens descritos no contexto do projeto.
- O parser antigo abre o log como ZIP e seleciona eventos por texto/data fixa; tais pressupostos precisam ser confrontados com arquivos oficiais atuais e documentação antes de qualquer uso.
- Os CSVs publicados pela referência são dados derivados de terceiros e não entram na base analítica deste projeto.
- Uma requisição independente ao `aux.jws` piloto em `resultados.tse.jus.br` respondeu HTTP 200 e retornou conteúdo JWS compacto. Isso só confirma resposta do endpoint. A assinatura, payload, hashes, formatos de BU/log e chain of trust seguem sem validação.

Próximo passo: implementação independente de uma prova de conceito por seção, que preserve o manifesto/bytes originais e registre metadados HTTP e SHA-256 local antes de interpretar os dados.

## Esquema ASN.1 usado para diagnóstico do BU

`schemas/bu.asn1` foi obtido de `danarrib/TSEParser` (`TSE_Docs/spec/bu.asn1`, commit `bc208105decf6a7ccd0d66db35ab02bacbd8c9c9`), repositório licenciado sob GPL-3.0, compatível com a licença GPL-3.0 deste projeto. A especificação é usada apenas para decodificar o envelope e comparar a identificação da seção. O conteúdo interno do BU 2026 falha com incompatibilidade de tag; nenhum número de votação é derivado desse schema. Precisamos localizar a especificação correspondente ao formato 2026 antes de reconciliar contagens.
