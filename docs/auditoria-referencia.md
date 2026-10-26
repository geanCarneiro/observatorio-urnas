# Auditoria inicial de `urnas-tardias`

Data da inspeção: 2026-10-26. Referência inspecionada: `Jonnes-Santos/urnas-tardias`, commit `b650fd02d89e691b2eef1a0a5a67fbe17da6619c`.

- O commit inclui a implementação do dashboard e o pipeline de coleta/análise herdado da referência `apolinario/urnas-tardias`.
- O checkout inspecionado não contém arquivo `LICENSE`. Sem licença explícita, não adaptar nem copiar código do repositório. Usar apenas conceitos gerais e escrever implementação independente, salvo esclarecimento posterior do titular.
- O coletor observado acessa `.../arquivo-urna/3221`, busca arquivo auxiliar JSON (sufixo `.json` no código), seleciona apenas o último item de `hashes[]` e o primeiro log encontrado. Isso diverge do formato `.jws` e da necessidade de preservar e inspecionar todos os itens descritos no contexto do projeto.
- O parser antigo abre o log como ZIP e seleciona eventos por texto/data fixa; tais pressupostos precisam ser confrontados com arquivos oficiais atuais e documentação antes de qualquer uso.
- Os CSVs publicados pela referência são dados derivados de terceiros e não entram na base analítica deste projeto.
- Uma requisição independente ao `aux.jws` piloto em `resultados.tse.jus.br` respondeu HTTP 200 e retornou conteúdo JWS compacto. Isso só confirma resposta do endpoint. A assinatura, payload, hashes, formatos de BU/log e chain of trust seguem sem validação.

Próximo passo: implementação independente de uma prova de conceito por seção, que preserve o manifesto/bytes originais e registre metadados HTTP e SHA-256 local antes de interpretar os dados.

## Esquema oficial do BU 2026

A fonte compatível é o pacote **Formato dos arquivos de BU, RDV e assinatura digital**, publicado na página oficial das Eleições 2026 do TSE: `https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/formato-arquivos-de-bu-rdv-e-assinatura-digital`. O ZIP observado tem SHA-256 `b5ac413e81121d53de2bb1657af611f84a45fdc84388e9d9821872994bbfbafd`; `spec/bu.asn1` tem SHA-256 `ef64bf723f774403b423ed0b617f5a83a3f50c0023d0003e18cd29170404ab81`. `schemas/bu.asn1` contém essa cópia oficial e o relatório verifica seu hash.

O schema antigo usado no primeiro decoder divergia em campos do envelope `Carga`, comparecimento e resultados. A aplicação agora decodifica o envelope e o conteúdo com o schema oficial, confirma a identidade da seção, compara habilitações/votos com comparecimento e totais por cargo, registra diferenças numéricas e expõe o histórico de códigos de carga sem revelar os próprios códigos. A validação da assinatura do JWS e a determinação do fuso são controles separados; decodificar o payload não equivale a verificar a assinatura.
