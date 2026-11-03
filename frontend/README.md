# Dashboard nativo

Aplicativo desktop em C# / WPF para acompanhar e controlar o job local atual/mais recente. O motor de execução continua em Python; o dashboard inicia o servidor local sem abrir o navegador, lê snapshots dos workers e reutiliza a API existente.

Na raiz do repositório, execute `abrir-dashboard-nativo.bat`. O launcher compila a versão atual em Release e abre a janela do dashboard; se a compilação falhar, mantém a mensagem visível.

Também é possível iniciá-lo pelo terminal:

```powershell
dotnet run --project frontend/ObservatorioUrnasDashboard/ObservatorioUrnasDashboard.csproj
```

O cabeçalho exibe somente o progresso geral. Os cards distinguem workers executando, aguardando tarefa e job pausado. Enquanto aguardam, preservam a última seção e etapa, o resultado e o tempo desde a conclusão.

**Novo job** abre uma janela para configurar coleta incremental/atualização bruta, processamento completo por remessas ou reprocessamento local. Coleta e processamento exigem prévia antes do início; o processamento completo também pede confirmação explícita da limpeza de `dados/`.

**Histórico de execuções** abre uma janela filha com resumo, parâmetros, contagens, plano, remessas, atividade paginada sob demanda e páginas de seções em tabelas. As células das tabelas permitem seleção e cópia; eventos de falha mostram PID, parâmetros do worker e traceback. A ação de copiar parâmetros fecha o histórico e abre **Novo job** preenchido; ela nunca inicia automaticamente uma execução antiga. A navegação carrega um resumo compacto do plano, sem trazer para a interface os inventários/checkpoints completos.

Os controles da home retomam ou solicitam parada segura apenas para o job atual/mais recente. **Abrir interface completa** abre a UI web no navegador para os comandos detalhados que ainda não foram migrados.
