# Piloto configurado: Cariacica/ES, zona 0054, seção 0509

Esta página documenta a configuração inicial do piloto da aplicação. Ela não registra resultado de execução. A seção é o padrão da workflow manual; os campos podem ser alterados para outra seção.

## Organização dos dados

- Guardar uma linha por evento permitido, timestamp original, posição no arquivo e hash da origem.
- Manter separados habilitação, voto computado, confirmações por cargo e comando/início/conclusão de encerramento. Mensagens brutas e identificadores pessoais ficam excluídos.
- Preservar eventos um a um com timestamp original, data e hora. A extração não aplica cortes horários nem consolida eventos; os horários refletem o relógio do arquivo até que seu fuso seja validado.
- Verificar os hashes dos arquivos contra o registro da aquisição, integridade ZIP e identificação da seção no envelope BU.
- Registrar estado explícito para fuso, assinatura, possíveis substituições e reconciliação de contagens. Falhas de schema deixam contagens BU e divergências como indisponíveis; não presumem equivalência.

O workflow manual fica em `.github/workflows/piloto-secao.yml`. Os originais e suas extrações JSON são publicados juntos, organizados por seção e pelo `idPleito` identificado no BU. A aplicação não faz coleta nacional nesta configuração.

## Fonte da amostra

Página do TSE fornecida para o piloto: Cariacica/ES, município 56251, zona 0054, seção 0509. O coletor consulta `aux.jws` da seção e baixa arquivos dos tipos `log`, `bu` e `rdv`. Os arquivos originais ficam juntos das extrações JSON granulares e de `acquisition.json`, que mantém manifesto e proveniência. `vota.vsc` permanece fora da coleta.
