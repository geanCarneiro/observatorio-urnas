# Piloto de coleta: Cariacica/ES, zona 0054, seção 0509

## Escopo

Coleta pontual solicitada para a seção exibida na página oficial do TSE. Nenhuma execução nacional foi iniciada. A coleta baixa somente o manifesto `aux.jws` e os tipos `log` e `bu`; RDV e `vota.vsc` são ignorados.

## Evidência da aquisição

- Endpoint oficial consultado: `https://resultados.tse.jus.br/oficial/ele2026/arquivo-urna/3221/dados/es/56251/0054/0509/p003221-es-m56251-z0054-s0509-aux.jws`
- Resposta do manifesto: HTTP 200, `Content-Type: application/json`; o corpo tem três segmentos compactos JWS.
- SHA-256 do manifesto: `ebbac6a93a2fb4fb62a72505c35c81c9873dbc8c4a5ac232995702dd16e1b410`.
- Cabeçalho declara `alg=EdDSA` e um `kid`; a assinatura não foi validada e permanece explicitamente `unverified`.
- O manifesto lista log, BU, RDV e `vota.vsc`. Foram baixados apenas o log e o BU.
- `o03221es5625100540509-log.jez`: 211.094 bytes, SHA-256 `89b32fa09a4a8da0be445fc96744689ca349cd5038eed55aa3eb27a70942545e`.
- O `.jez` tem assinatura ZIP válida, um membro `logd.dat` de 1.152.355 bytes e CRC íntegro. O log contém registros em múltiplas datas, incluindo 25/10/2026; ainda não se atribuiu semântica aos eventos.
- `o03221es5625100540509-bu.dat`: 1.543 bytes, SHA-256 `1061fe31018f709f86624b8cc7ef21db8aa5d842b6c1dde92ad95f91cb6cfb36`; formato ainda não decodificado.
- Manifesto, arquivos e `collection.json` ficam em `dados/es/56251/0054/0509/runs/<aquisição-UTC>/`; `dados/` é ignorado pelo Git.

Os hashes identificam os bytes observados nesta aquisição e podem mudar em uma coleta posterior. Consultar `collection.json` para URLs, status e cabeçalhos HTTP completos.

## Próximas verificações

1. Obter documentação/chaves oficiais e validar a assinatura JWS, sem confundir decodificação do payload com autenticação.
2. Encontrar schema BU compatível com este payload e reconciliar comparecimento por cargo com os eventos correspondentes do log.
3. Conferir manifestos com múltiplos hashes e as relações entre substituições/versões.
4. Só depois do piloto validado, dimensionar armazenamento e duração para lotes persistentes no GitHub Actions. O modo nacional permanece sem execução.

## Resumo inicial do log

O analisador independente seleciona uma lista fechada de nomes de eventos e descarta as demais colunas e mensagens. Para 25/10/2026, encontrou 311 habilitações, 311 eventos `O voto do eleitor foi computado`, 311 confirmações para cada cargo e um início/comando/confirmação de encerramento. Também há registros selecionados em 04/10/2026 (296 habilitações e 296 eventos de voto); as datas são mantidas separadas.

Os horários aparecem no relatório como foram gravados no log. Na aquisição atual, a conclusão do encerramento do log (17:00:28) coincide com o horário de encerramento do BU (17:00:28); isso confirma concordância entre relógios da mesma urna, mas não determina fuso ou offset UTC. A ordem do arquivo é comando às 17:00:06, conclusão às 17:00:28 e evento chamado “Inicio do Encerramento” às 17:02:15; o relatório preserva os timestamps e a ordem de linha. A sequência dos nomes de evento difere da sequência semântica esperada, embora os horários sejam monotônicos; portanto, não tratamos “Inicio do Encerramento” como equivalente ao comando ou à conclusão.

Foram selecionados 311 eventos `O voto do eleitor foi computado` em 25/10/2026 e 296 em 04/10/2026. Não há habilitação ou voto registrado após 17:00 na escala bruta do log em 25/10; não traduzir isso para horário de Brasília ainda. A reconciliação numérica com BU permanece indisponível: o envelope ASN.1 identifica corretamente a seção e sua fase, mas o schema interno disponível falha no payload de 2026. O relatório deixa contagens BU e divergências como `null`, sem inferir equivalência.

O artefato anterior agregado foi mantido em `reports/piloto-secao.json`; a nova versão com eventos individuais, timestamps completos, cortes configuráveis e estados de validação está em `reports/piloto-secao-v2.json`.


A comparação campo a campo entre o primeiro e o segundo artefato está em `docs/comparacao-piloto-v1-v2.md`.
