# Execuções por seção e nacionais

## Objetivo

O mesmo coletor deve aceitar uma seção explícita para desenvolvimento e validação, além de um modo nacional que possa ser acionado manualmente no GitHub Actions. O modo nacional será implementado, mas não executado até a prova de conceito por seção ser validada e aprovada.

## Interface planejada

- `section`: recebe UF, município, zona e seção como strings, preservando zeros à esquerda. É o modo padrão para desenvolvimento e reproduz exatamente uma unidade de coleta.
- `national`: recebe um inventário oficial versionado/identificado, particiona seções em lotes retomáveis e registra progresso e falhas por seção/arquivo.
- Ambos usam a mesma descoberta de manifesto, download, validação e armazenamento bruto. O seletor altera o inventário de entrada, não a semântica do processamento.

## GitHub Actions

- Workflow manual (`workflow_dispatch`) com escolha explícita `section` ou `national`.
- O modo `section` exige os quatro identificadores e executa somente essa seção.
- O modo `national` exige aprovação por proteção de ambiente do GitHub, usa inventário oficial congelado por versão/hash e divide a carga em lotes independentes com checkpoints externos duráveis.
- Artefatos e resumos registram commit do código, versão do inventário, parâmetros, cobertura, horários, erros e hashes dos arquivos brutos.
- Não publicar dados sensíveis de RDV ou `vota.vsc`; coleta desses formatos fica desativada até justificativa e revisão específicas.
- A workflow será habilitada para execução nacional somente após medir duração, armazenamento, limites do Actions e taxa segura no piloto. Filesystem efêmero do runner não é checkpoint.

## Portões de validação

1. Auditar licença e decisões do projeto de referência; não importar dados derivados.
2. Revalidar o manifesto oficial da seção piloto, interpretar o formato assinado e sua cadeia de confiança, listar todos os hashes/arquivos e baixar BU e log sem substituir bytes originais.
3. Confirmar checksum local, metadados HTTP, armazenamento atômico, retomada/idempotência e parser sem presumir nomes de eventos.
4. Só então medir custo e habilitar uma execução nacional protegida no Actions. A configuração do workflow, por si só, não autoriza nem inicia essa execução.

## Limites analíticos

Preservar timestamps brutos; não classificar automaticamente uma urna por um único corte de horário. Correlação não demonstra causalidade, intenção ou irregularidade. Não atribuir voto individual a candidato ou horário. Toda métrica precisa expor cobertura, exclusões, definições e incerteza.
