# Consolidação determinística por remessas

## Objetivo

Processar aquisições localmente em grupos limitados, produzir uma linha compacta por seção/turno e limpar os arquivos brutos antes de avançar. A rotina não usa IA e não grava extrações JSON detalhadas de BU, RDV ou log.

## Ciclo e armazenamento

- Estado da execução, inventário, plano e retomada: `.estado-local/runs/<run_id>/`.
- Dados brutos temporários de uma remessa: `dados/`.
- Resultado durável, separado por estado: `consolidados/<run_id>/<UF>/remessa-NNNNN/consolidacao.csv` e `manifest.json`.
- Índice final de cada remessa: `consolidados/<run_id>/_remessas/remessa-NNNNN.json`.
- Evidência bruta que exige inspeção: `consolidados/<run_id>/evidencias/remessa-NNNNN/<uf>/<municipio>/<zona>/<secao>/`.

O início exige confirmação para limpar todo o conteúdo de `dados/`. Após cada remessa, a limpeza só ocorre depois de gravar atomicamente o CSV e o manifest e preservar evidências de anomalia verdadeira/indeterminada ou processamento incompleto. Uma falha antes dessa etapa mantém os brutos para retomada.

Uma única fila contínua contém as seções da remessa. Cada worker executa o fluxo completo de uma seção: coleta dos arquivos brutos quando necessário, análise determinística e gravação do resumo compacto em SQLite. Assim, enquanto um worker coleta, outros podem analisar seções diferentes. O número configurado limita quantas seções avançam simultaneamente, não uma etapa específica. O uso de memória cresce com a quantidade de workers e com o tamanho das aquisições processadas simultaneamente.

Quando todas as seções da remessa têm um resumo persistido, um worker finalizador reúne os resultados, grava um CSV e manifesto para cada estado presente e cria um índice da remessa. A limpeza dos dados brutos só ocorre depois dos arquivos por estado, do índice e das evidências necessárias estarem duráveis.

## Proxy de horário regular

A regra é versionada como `queue-proxy-ba-matrix-a1-c14-v1` e registra a área selecionada `A1:C14` da matriz definida para BA.

- Confirmações explícitas de Presidente até 17:00, inclusive, pertencem ao proxy regular.
- Confirmações presidenciais após 17:00 pertencem ao proxy de fila se o início do encerramento registrado no log estiver entre 17:00 e 17:29:59 e a última confirmação explícita de qualquer cargo ocorrer de 0 a 12:59 antes dele.
- Confirmações presidenciais posteriores a 17:00 fora dessa área contam como votos fora do proxy.
- Se houver confirmações após 17:00 e faltar timestamp válido de confirmação ou de início do encerramento, o total posterior fica em `votos_pos_17_sem_classificacao`.

Os timestamps do log são usados como registrados. O fuso ainda não foi validado independentemente. O CSV mantém horários brutos, intervalo em segundos, classificação e versão da regra.

## Reconciliação e votos

`votos_confirmados_presidente_log` conta eventos explícitos `Voto confirmado para [Presidente]`, no dia da eleição e após a deduplicação de fluxos completos já implementada. A atribuição de voto nulo após suspensão permanece separada e não é somada à contagem de confirmações. Eventos e sessões continuam disponíveis nos arquivos de evidência quando a comparação exige inspeção.

`flag_anomalia_log_rdv_bu` compara a contagem de confirmações explícitas com os totais presidenciais do RDV e do BU. O campo vale `true` se qualquer comparação divergir, `false` se ambas estiverem disponíveis e coincidirem, ou `indeterminado` se faltar uma delas. As diferenças `log − RDV`, `log − BU` e `RDV − BU` são campos separados. Falhas de processamento não são tratadas como ausência de anomalia.

Os votos dos candidatos 22 e 13 são derivados das contagens nominais do RDV, usando BU como fallback quando RDV não estiver disponível. O CSV preserva os totais das duas fontes e indica a base escolhida. A proporção válida é `votos do candidato ÷ total de votos válidos × 100`. O voto normalizado é `votos válidos do candidato × votos dentro do proxy ÷ total de votos válidos`; o percentual normalizado divide esse resultado pelo total de votos dentro do proxy. Como o ajuste é proporcional, essa porcentagem reproduz a proporção válida observada. Percentuais sem denominador positivo ficam vazios.

## Conteúdo consolidado

Cada linha representa uma seção/turno e contém UF, código TSE e nome do município, zona, seção, turno, classificação de proxy, votos presidenciais dentro/fora do proxy, totais presidenciais do log/RDV/BU, diferenças e flag de anomalia, totais/percentuais/normalizações para 22 e 13, status de processamento, data/pleito e hashes das fontes.

O manifest registra quantidade de linhas, colunas, hash do CSV, hashes dos schemas e versões das regras. As linhas são ordenadas por UF, município, zona, seção e turno.
