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
2. Identificar formalmente os formatos `.jez` e BU e documentar o significado de timestamps/eventos antes de gerar métricas.
3. Conferir manifestos com múltiplos hashes e as relações entre substituições/versões.
4. Só depois do piloto validado, dimensionar armazenamento e duração para lotes persistentes no GitHub Actions. O modo nacional permanece sem execução.

## Resumo inicial do log

O analisador independente seleciona uma lista fechada de nomes de eventos e descarta as demais colunas e mensagens. Para 25/10/2026, encontrou 311 habilitações, 311 eventos `O voto do eleitor foi computado`, 311 confirmações para cada cargo e um início/comando/confirmação de encerramento. Também há registros selecionados em 04/10/2026 (296 habilitações e 296 eventos de voto); as datas são mantidas separadas.

Os horários aparecem no relatório como foram gravados no log. Não há contagens após 17h na escala do relógio bruto nesta amostra; não converter nem comparar com horário de Brasília antes de validar o fuso/relógio. Esses totais do log tampouco foram reconciliados com o BU: o `.dat` segue sem parser/schema validado. O resumo gerado está em `reports/piloto-secao.json`.
