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

A fonte compatível é o pacote **Formato dos arquivos de BU, RDV e assinatura digital**, publicado na página oficial das Eleições 2026 do TSE: `https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/formato-arquivos-de-bu-rdv-e-assinatura-digital`. O ZIP observado tem SHA-256 `b5ac413e81121d53de2bb1657af611f84a45fdc84388e9d9821872994bbfbafd`; `spec/bu.asn1` tem SHA-256 `ef64bf723f774403b423ed0b617f5a83a3f50c0023d0003e18cd29170404ab81`. `schemas/bu.asn1` contém essa cópia oficial e a extração registra o hash do schema usado.

O schema antigo usado no primeiro decoder divergia em campos do envelope `Carga`, comparecimento e resultados. A aplicação agora decodifica o envelope e o conteúdo com o schema oficial, confirma a identidade da seção, compara habilitações/votos com comparecimento e totais por cargo, registra diferenças numéricas e expõe o histórico de códigos de carga sem revelar os próprios códigos. A validação da assinatura do JWS e a determinação do fuso são controles separados; decodificar o payload não equivale a verificar a assinatura.

## Esquema oficial do RDV 2026

`schemas/rdv.asn1` é a cópia `spec/rdv.asn1` do mesmo pacote oficial. O SHA-256 do pacote ZIP é `b5ac413e81121d53de2bb1657af611f84a45fdc84388e9d9821872994bbfbafd`; o SHA-256 do schema RDV é `282dcde0c7ba1f1bb128fe27443ad8d18f7490416c9546d033910a7ec557a465`. O JSON extraído do RDV registra os hashes observados e o schema usado.

O decoder aceita `EntidadeRegistroDigitalVoto` e `EntidadeResultadoRDV`, diretamente ou dentro de `EntidadeEnvelopeGenerico` com `tipoEnvelope=envelopeRegistroDigitalVoto`. Envelope com segurança/cifra não é decifrado: a análise falha explicitamente e o coletor já preservou os bytes originais. O RDV inteiro é retido sem alteração no diretório de aquisição; seu JSON guarda SHA-256, tamanho e TLV BER original em hexadecimal para cada registro. A integridade estrutural é verificada pelo parser BER com limites estritos, e cada TLV de voto precisa decodificar novamente para o mesmo valor produzido pelo decoder ASN.1.

| Campo oficial | Preservação e interpretação |
| --- | --- |
| `pleito`, `fase`, `identificacao` | Metadados do registro digital, fase e município/zona/local/seção conforme ASN.1. |
| `historicoCodigosCarga` | Sequência integral de códigos de carga, preservada no metadado do RDV para análise de substituições. |
| `eleicoes` / `idEleicao` | Variante Vota ou SA e identificador de cada eleição; SA também retém `tipoCedulaSA` e `origemVotosSA`. |
| `idCargo`, `quantidadeEscolhas` | Tipo/código oficial do cargo e quantidade máxima de escolhas, sem substituir os códigos por rótulos externos. |
| `Voto.tipoVoto` | Tipo enumerado oficial, preservado por registro e contado individualmente por cargo. Brancos e nulos também recebem totais amplos pelos prefixos semânticos do enum, mantendo as variantes separadas. |
| `Voto.digitacao` | Valor textual exato quando presente. O ASN.1 oficial define `VotoDigitado` como a digitação feita na urna (1 a 5 caracteres), enquanto o ASN.1 do BU define `NumeroPartido` no intervalo 0–99. Para `legenda`, os dois primeiros dígitos são expostos como prefixo partidário para comparação com o BU; a digitação completa continua preservada em cada registro e em uma contagem própria. Essa derivação não substitui nem altera o valor bruto. Em outros tipos o campo fica preservado sem interpretação adicional. |
| Posição do registro | Índice da sequência dentro da eleição e cargo. Descreve somente a posição serializada; não é horário, ordem cronológica nem chave compartilhada entre cargos. |
| TLV BER de cada voto | Bytes exatos da codificação ASN.1 do registro individual, junto ao objeto estruturado. |

A reconciliação compara total de registros por cargo, números nominais, prefixos partidários das digitações de legenda e totais branco/nulo com as categorias do BU que têm correspondência agregada explícita. Cada diferença é registrada. Os tipos específicos de suspensão permanecem visíveis individualmente; o agrupamento amplo para comparar branco/nulo segue o nome definido no enum do RDV. Categorias sem par inequívoco no BU não são silenciosamente convertidas. Um status `partial` indica cargo sem par; `divergence` indica total ou subtotal incompatível.

O JSON extraído junto ao RDV inclui os registros individuais e deve ser tratado como dado de votação com granularidade alta. O workflow retém os originais e extrações por até 90 dias no GitHub Actions; os originais são acompanhados por `manifest.aux.jws` e `acquisition.json`. Nem a posição do voto nem qualquer outro campo do RDV é associado a eleitor, horário de habilitação ou evento do log. Nenhuma inferência temporal é feita a partir da ordem.

Limitações: a assinatura JWS ainda não é verificada, e o RDV cifrado não é decifrado; conversões entre enumerações com semânticas diferentes são deliberadamente evitadas; a validação não prova a autenticidade do emissor apenas pela conformidade ASN.1/SHA-256 local.
