# Piloto configurado: Cariacica/ES, zona 0054, seção 0509

Esta página documenta a configuração inicial do piloto da aplicação. Ela não registra resultado de execução. A seção é o padrão da workflow manual; os campos podem ser alterados para outra seção.

## Comportamento esperado do artefato

- Guardar uma linha por evento permitido, timestamp original, posição no arquivo e hash da origem.
- Manter separados habilitação, voto computado, confirmações por cargo e comando/início/conclusão de encerramento. Mensagens brutas e identificadores pessoais ficam excluídos.
- Separar as datas e calcular atividade a partir de cortes de relógio configuráveis. Os cortes refletem o relógio do arquivo até que seu fuso seja validado.
- Verificar os hashes dos arquivos contra o registro da aquisição, integridade ZIP e identificação da seção no envelope BU.
- Registrar estado explícito para fuso, assinatura, possíveis substituições e reconciliação de contagens. Falhas de schema deixam contagens BU e divergências como indisponíveis; não presumem equivalência.

O workflow manual fica em `.github/workflows/piloto-secao.yml`. O relatório é publicado como artefato da execução do GitHub Actions; a aplicação não faz coleta nacional nesta configuração.

## Fonte da amostra

Página do TSE fornecida para o piloto: Cariacica/ES, município 56251, zona 0054, seção 0509. O coletor consulta `aux.jws` da seção e baixa arquivos dos tipos `log`, `bu` e `rdv`. O workflow exige ao menos um RDV, gera análise e preserva os bytes originais junto ao manifesto e metadados de aquisição em artefato próprio. `vota.vsc` permanece fora da coleta.
