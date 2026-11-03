# Auditoria e conciliação determinísticas do conjunto local

## Objetivo

Reutilizar resultados de seção confirmados, validar somente sessões pendentes cujo estado físico seja necessário e coletar ou conciliar o que faltar. A execução produz uma linha compacta por seção/turno e não usa IA nem grava extrações JSON detalhadas de BU, RDV ou log.

## Ciclo e armazenamento

- Estado da execução, inventário, plano, auditoria e retomada: `.estado-local/runs/<run_id>/`.
- Resumo e decisões físicas da auditoria: `audit-summary.json` e `audit-results.jsonl` dentro da pasta da execução.
- Dados brutos: `dados/`.
- Resultado durável, separado por estado, município e zona: `consolidados/<run_id>/<UF>/<municipio>/<zona>.csv` e manifesto correspondente.
- Evidência bruta que exige inspeção: `consolidados/<run_id>/evidencias/<uf>/<municipio>/<zona>/<secao>/`.

Os dados brutos são temporários: após a consolidação segura e a preservação de evidências necessárias, a árvore da seção é removida. Um job novo pode reutilizar resultados compatíveis do job por remessas anterior, que ficam em `.estado-local` e `consolidados`, fora de `dados/`. A consolidação confirmada é registrada por seção/turno no SQLite da execução, com hash do resumo; esse marcador permite reutilizar o resultado sem repetir auditoria física quando não há estado físico incerto. Execuções antigas sem o marcador são migradas a partir de resumos válidos já persistidos.

Workers paralelos auditam somente sessões pendentes com dados físicos ou estado incerto por falha/interrupção. Se uma sessão pendente não possui árvore física nem tentativa incerta, ela vai diretamente para coleta; resultados confirmados são reutilizados do estado persistido, não de `dados/`. Na retomada, o job mantém seu próprio estado e verifica somente sessões sem marcador confiável ou com arquivos físicos que ainda precisem de validação antes da limpeza. A parada segura registra os estados concluídos; após queda do processo ou da máquina, a próxima execução coleta e reconcilia novamente as seções incertas.

Depois da verificação direcionada, as seções que precisam de coleta ou conciliação entram na fila. Os workers continuam sendo usados em paralelo; a fila é alimentada por checkpoints internos para limitar tarefas residentes na memória. Esses checkpoints não são fases ou remessas visíveis na UI. Cada seção tem no máximo uma etapa ativa por vez, pois seus arquivos de log são compartilhados pelos turnos. Uma coleta necessária é seguida da conciliação determinística dessa seção; se os dados brutos já estiverem íntegros, apenas a conciliação necessária é executada.

Os resultados por seção/turno são gravados no SQLite local da execução. Ao final, um finalizador escreve os CSVs finais por estado, município e zona e o manifesto geral. A limpeza de cada árvore bruta exige validação física adicional e só ocorre depois que os resumos e as evidências necessárias estiverem duráveis. Falhas de auditoria ou processamento deixam a execução parcial e preservam os dados necessários para inspeção ou retomada.

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
