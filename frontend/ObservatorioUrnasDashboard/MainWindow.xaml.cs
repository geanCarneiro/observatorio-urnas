using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Windows;
using System.Windows.Threading;
using ObservatorioUrnasDashboard.Models;
using ObservatorioUrnasDashboard.Services;

namespace ObservatorioUrnasDashboard;

public partial class MainWindow : Window, INotifyPropertyChanged
{
    private readonly PythonUiBackend _backend = new();
    private readonly CancellationTokenSource _lifetime = new();
    private readonly DispatcherTimer _snapshotTimer = new() { Interval = TimeSpan.FromSeconds(1) };
    private readonly DispatcherTimer _workerRefreshTimer = new() { Interval = TimeSpan.FromMilliseconds(120) };
    private FileSystemWatcher? _workerWatcher;
    private string? _watchedRunId;
    private string? _repositoryRoot;
    private DashboardSnapshot? _snapshot;
    private bool _refreshInProgress;
    private bool _startupComplete;
    private double _progressPercent;
    private string _progressLabel = "—";
    private string _connectionText = "Iniciando controle local…";
    private string _workerSummary = "Carregando estado local…";
    private string _runTitle = "";
    private string _runId = "";
    private string _runStatus = "";
    private string _statusMessage = "";
    private string _activityText = "Aguardando atividade.";
    private bool _canResume;
    private bool _canStop;
    private bool _canCreateJob;

    public MainWindow()
    {
        InitializeComponent();
        DataContext = this;
        _snapshotTimer.Tick += async (_, _) => await RefreshSnapshotAsync();
        _workerRefreshTimer.Tick += (_, _) =>
        {
            _workerRefreshTimer.Stop();
            RefreshWorkerFiles();
        };
    }

    public ObservableCollection<WorkerCard> WorkerCards { get; } = [];
    public double ProgressPercent { get => _progressPercent; set => Set(ref _progressPercent, value); }
    public string ProgressLabel { get => _progressLabel; set => Set(ref _progressLabel, value); }
    public string ConnectionText { get => _connectionText; set => Set(ref _connectionText, value); }
    public string WorkerSummary { get => _workerSummary; set => Set(ref _workerSummary, value); }
    public string RunTitle { get => _runTitle; set => Set(ref _runTitle, value); }
    public string RunId { get => _runId; set => Set(ref _runId, value); }
    public string RunStatus { get => _runStatus; set => Set(ref _runStatus, value); }
    public string StatusMessage { get => _statusMessage; set => Set(ref _statusMessage, value); }
    public string ActivityText { get => _activityText; set => Set(ref _activityText, value); }
    public bool CanResume { get => _canResume; set => Set(ref _canResume, value); }
    public bool CanStop { get => _canStop; set => Set(ref _canStop, value); }
    public bool CanCreateJob { get => _canCreateJob; set => Set(ref _canCreateJob, value); }

    public event PropertyChangedEventHandler? PropertyChanged;

    private async void Window_Loaded(object sender, RoutedEventArgs e)
    {
        try
        {
            _repositoryRoot = FindRepositoryRoot(AppContext.BaseDirectory);
            ConnectionText = "Iniciando o servidor Python em segundo plano…";
            await _backend.StartAsync(_repositoryRoot, _lifetime.Token);
            _startupComplete = true;
            ConnectionText = $"Conectado ao motor local · {_backend.BaseUri}";
            await RefreshSnapshotAsync();
            _snapshotTimer.Start();
        }
        catch (Exception error)
        {
            ConnectionText = "Falha ao conectar ao motor local";
            StatusMessage = error.Message;
            CanResume = false;
            CanStop = false;
        }
    }

    private async Task RefreshSnapshotAsync()
    {
        if (!_startupComplete || _refreshInProgress || _lifetime.IsCancellationRequested) return;
        _refreshInProgress = true;
        try
        {
            var state = await _backend.GetSnapshotAsync(_lifetime.Token);
            _snapshot = state;
            RunTitle = state.RunTitle;
            RunId = state.RunId ?? "";
            RunStatus = state.RunStatus;
            StatusMessage = state.StatusMessage;
            CanResume = state.CanResume;
            CanStop = state.CanStop;
            CanCreateJob = state.CanCreateJob;
            ProgressPercent = state.Progress ?? 0;
            ProgressLabel = state.Progress is null ? "—" : $"{state.Progress:0.00}%";
            ActivityText = state.ActivityText;
            WorkerSummary = state.RunId is null
                ? "Nenhum processo worker configurado."
                : $"{WorkerCards.Count(card => card.ProcessId.HasValue)} processo(s) observado(s) · {state.WorkerLimit} configurado(s)";
            ConfigureWorkerWatcher(state.RunId);
            RefreshWorkerFiles();
        }
        catch (OperationCanceledException) { }
        catch (Exception error)
        {
            ConnectionText = "Conexão local temporariamente indisponível";
            StatusMessage = error.Message;
        }
        finally
        {
            _refreshInProgress = false;
        }
    }

    private void ConfigureWorkerWatcher(string? runId)
    {
        if (string.Equals(runId, _watchedRunId, StringComparison.Ordinal)) return;
        _workerWatcher?.Dispose();
        _workerWatcher = null;
        _watchedRunId = runId;
        if (string.IsNullOrWhiteSpace(runId) || _repositoryRoot is null) return;

        var workerDirectory = Path.Combine(_repositoryRoot, ".estado-local", "runs", runId, "workers");
        Directory.CreateDirectory(workerDirectory);
        _workerWatcher = new FileSystemWatcher(workerDirectory, "worker-*.json")
        {
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.Size,
            IncludeSubdirectories = false,
            EnableRaisingEvents = true,
        };
        _workerWatcher.Created += WorkerFileChanged;
        _workerWatcher.Changed += WorkerFileChanged;
        _workerWatcher.Renamed += WorkerFileChanged;
        _workerWatcher.Deleted += WorkerFileChanged;
        _workerWatcher.Error += (_, _) => ScheduleWorkerRefresh();
        ScheduleWorkerRefresh();
    }

    private void WorkerFileChanged(object sender, FileSystemEventArgs e) => ScheduleWorkerRefresh();

    private void ScheduleWorkerRefresh()
    {
        Dispatcher.BeginInvoke(() =>
        {
            _workerRefreshTimer.Stop();
            _workerRefreshTimer.Start();
        }, DispatcherPriority.Background);
    }

    private void RefreshWorkerFiles()
    {
        var state = _snapshot;
        if (state?.RunId is null || _repositoryRoot is null) return;
        var workerDirectory = Path.Combine(_repositoryRoot, ".estado-local", "runs", state.RunId, "workers");
        var snapshots = new List<WorkerSnapshot>();
        if (Directory.Exists(workerDirectory))
        {
            foreach (var file in Directory.EnumerateFiles(workerDirectory, "worker-*.json"))
            {
                try
                {
                    using var json = JsonDocument.Parse(File.ReadAllText(file));
                    var root = json.RootElement;
                    if (StringValue(root, "run_id") != state.RunId) continue;
                    var pid = root.TryGetProperty("process_id", out var pidValue) && pidValue.TryGetInt32(out var processId)
                        ? processId : (int?)null;
                    if (pid is null) continue;
                    snapshots.Add(new WorkerSnapshot(
                        WorkerId: StringValue(root, "worker_id") ?? $"pid-{pid}",
                        ProcessId: pid.Value,
                        Status: StringValue(root, "status") ?? "idle",
                        Stage: StringValue(root, "stage") ?? "",
                        Section: FormatSection(TextOrSection(root, "section"), StringValue(root, "turn")),
                        Progress: NumberValue(root, "progress") ?? 0,
                        UpdatedAt: StringValue(root, "updated_at_utc") ?? "",
                        LastTaskStage: StringValue(root, "last_task_stage") ?? "",
                        LastTaskSection: FormatSection(TextOrSection(root, "last_task_section"),
                            StringValue(root, "last_task_turn")),
                        LastTaskFile: StringValue(root, "last_task_file") ?? "",
                        LastTaskOutcome: StringValue(root, "last_task_outcome") ?? "",
                        LastTaskCompletedAt: StringValue(root, "last_task_completed_at_utc") ?? "",
                        FinishedTaskCount: root.TryGetProperty("finished_task_count", out var countValue)
                            && countValue.TryGetInt32(out var count) ? count : 0));
                }
                catch (IOException) { }
                catch (JsonException) { }
            }
        }
        UpdateWorkerCards(snapshots, state.WorkerLimit, state.RunStatus);
    }

    private void UpdateWorkerCards(List<WorkerSnapshot> snapshots, int configuredCount, string runStatus)
    {
        if (configuredCount <= 0)
        {
            WorkerCards.Clear();
            WorkerSummary = "Nenhum processo worker registrado neste job.";
            return;
        }

        var selected = snapshots.OrderByDescending(item => ParseTimestamp(item.UpdatedAt))
            .Take(configuredCount).OrderBy(item => item.ProcessId).ToList();
        var identities = selected.Select(item => item.WorkerId).ToList();
        for (var slot = selected.Count; slot < configuredCount; slot++) identities.Add($"slot-{slot + 1}");

        if (!WorkerCards.Select(item => item.WorkerId).SequenceEqual(identities))
        {
            WorkerCards.Clear();
            foreach (var identity in identities) WorkerCards.Add(new WorkerCard { WorkerId = identity });
        }

        for (var index = 0; index < WorkerCards.Count; index++)
        {
            var card = WorkerCards[index];
            if (index < selected.Count)
            {
                var worker = selected[index];
                var jobActive = runStatus is "running" or "preparing";
                var workerRunning = string.Equals(worker.Status, "running", StringComparison.OrdinalIgnoreCase);
                var running = jobActive && workerRunning;
                var hasLastTask = worker.FinishedTaskCount > 0;
                var isPaused = runStatus == "paused";
                card.Label = $"Worker {index + 1:00} · PID {worker.ProcessId}";
                card.ProcessId = worker.ProcessId;
                card.Status = running ? "Executando" : isPaused ? "Pausado" : jobActive ? "Aguardando" : "Encerrado";
                if (running)
                {
                    card.Stage = worker.Stage;
                    card.Section = worker.Section;
                    card.ProgressCaption = "Progresso";
                    card.Progress = worker.Progress;
                    card.ProgressText = $"{worker.Progress:0}%";
                }
                else if (hasLastTask)
                {
                    card.Stage = string.IsNullOrWhiteSpace(worker.LastTaskStage)
                        ? "Última tarefa concluída" : $"Última etapa: {worker.LastTaskStage}";
                    if (!string.IsNullOrWhiteSpace(worker.LastTaskFile))
                        card.Stage += $" · {Path.GetFileName(worker.LastTaskFile)}";
                    card.Section = worker.LastTaskSection;
                    card.ProgressCaption = "Última atividade";
                    card.Progress = 100;
                    card.ProgressText = $"{FormatTaskOutcome(worker.LastTaskOutcome)} · {FormatElapsed(worker.LastTaskCompletedAt)} · {worker.FinishedTaskCount} tarefa(s)";
                }
                else
                {
                    card.Stage = isPaused ? "Aguardando tarefa quando o job foi pausado"
                        : jobActive ? "Aguardando a primeira tarefa" : "Nenhuma tarefa registrada";
                    card.Section = "";
                    card.ProgressCaption = "Atividade";
                    card.Progress = 0;
                    card.ProgressText = "Sem tarefas concluídas";
                }
            }
            else
            {
                card.Label = $"Worker {index + 1:00}";
                card.ProcessId = null;
                card.Status = runStatus is "running" or "preparing" ? "Aguardando" : runStatus == "paused" ? "Pausado" : "Encerrado";
                card.Stage = runStatus is "running" or "preparing" ? "Processo ainda não iniciado" : "";
                card.Section = "";
                card.ProgressCaption = "Atividade";
                card.Progress = 0;
                card.ProgressText = "Sem atividade registrada";
            }
        }
        var finishedTasks = selected.Sum(item => item.FinishedTaskCount);
        WorkerSummary = $"{selected.Count} processo(s) observado(s) · {configuredCount} configurado(s) · {finishedTasks} tarefa(s) finalizada(s)";
    }

    private async void Resume_Click(object sender, RoutedEventArgs e)
    {
        if (_snapshot?.RunId is not { Length: > 0 } runId) return;
        try
        {
            StatusMessage = "Enviando solicitação de retomada…";
            CanResume = false;
            await _backend.ResumeAsync(runId, _lifetime.Token);
            await RefreshSnapshotAsync();
        }
        catch (Exception error) { StatusMessage = error.Message; }
    }

    private async void Stop_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            StatusMessage = "Solicitando parada segura…";
            CanStop = false;
            await _backend.StopAsync(_lifetime.Token);
            await RefreshSnapshotAsync();
        }
        catch (Exception error) { StatusMessage = error.Message; }
    }

    private void OpenBrowser_Click(object sender, RoutedEventArgs e)
    {
        if (_backend.BaseUri is null) return;
        Process.Start(new ProcessStartInfo(_backend.BaseUri.ToString()) { UseShellExecute = true });
    }

    private void NewJob_Click(object sender, RoutedEventArgs e) => OpenNewJobWindow();

    private void History_Click(object sender, RoutedEventArgs e)
    {
        var history = new RunHistoryWindow(_backend, _lifetime.Token) { Owner = this };
        if (history.ShowDialog() == true && history.ConfigurationToCopy is { } configuration)
            OpenNewJobWindow(configuration);
    }

    private void OpenNewJobWindow(NewJobConfiguration? configuration = null)
    {
        if (!CanCreateJob)
        {
            StatusMessage = "Já existe um job em andamento. Aguarde ou solicite a parada segura antes de criar outro.";
            return;
        }
        var window = new NewJobWindow(_backend, configuration, _lifetime.Token) { Owner = this };
        if (window.ShowDialog() == true) _ = RefreshSnapshotAsync();
    }

    private void Window_Closed(object? sender, EventArgs e)
    {
        _snapshotTimer.Stop();
        _workerRefreshTimer.Stop();
        _workerWatcher?.Dispose();
        _lifetime.Cancel();
        _backend.Dispose();
        _lifetime.Dispose();
    }

    private static string FindRepositoryRoot(string start)
    {
        var current = new DirectoryInfo(start);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "tools", "local_ui.py"))) return current.FullName;
            current = current.Parent;
        }
        throw new DirectoryNotFoundException("Não encontrei a raiz do repositório a partir da pasta do dashboard.");
    }

    private static string? StringValue(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() : null;

    private static string? TextOrSection(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value)) return null;
        if (value.ValueKind == JsonValueKind.String) return value.GetString();
        if (value.ValueKind != JsonValueKind.Object) return null;
        var parts = new[] { "uf", "municipio", "zona", "secao" }
            .Select(key => StringValue(value, key)).Where(part => !string.IsNullOrWhiteSpace(part));
        var formatted = string.Join("/", parts);
        return formatted.Length > 0 ? formatted : null;
    }

    private static double? NumberValue(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number
            ? value.GetDouble() : null;

    private static string FormatSection(string? section, string? turn)
    {
        if (string.IsNullOrWhiteSpace(section)) return "";
        return string.IsNullOrWhiteSpace(turn) ? section : $"{section} · turno {turn}";
    }

    private static string FormatTaskOutcome(string outcome) => outcome switch
    {
        "completed" => "Concluída",
        "partial" => "Concluída parcialmente",
        "failed" => "Falhou",
        "cancelled" => "Cancelada",
        _ => "Resultado registrado",
    };

    private static string FormatElapsed(string value)
    {
        if (!DateTimeOffset.TryParse(value, out var completedAt)) return "horário indisponível";
        var elapsed = DateTimeOffset.UtcNow - completedAt.ToUniversalTime();
        if (elapsed < TimeSpan.Zero) elapsed = TimeSpan.Zero;
        if (elapsed.TotalSeconds < 60) return $"há {(int)elapsed.TotalSeconds}s";
        if (elapsed.TotalMinutes < 60) return $"há {(int)elapsed.TotalMinutes} min";
        if (elapsed.TotalHours < 24) return $"há {(int)elapsed.TotalHours} h";
        return $"há {(int)elapsed.TotalDays} d";
    }

    private static DateTimeOffset ParseTimestamp(string value) =>
        DateTimeOffset.TryParse(value, out var result) ? result : DateTimeOffset.MinValue;

    private void Set<T>(ref T field, T value, [CallerMemberName] string? propertyName = null)
    {
        if (EqualityComparer<T>.Default.Equals(field, value)) return;
        field = value;
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }

    private sealed record WorkerSnapshot(string WorkerId, int ProcessId, string Status,
        string Stage, string Section, double Progress, string UpdatedAt, string LastTaskStage,
        string LastTaskSection, string LastTaskFile, string LastTaskOutcome,
        string LastTaskCompletedAt, int FinishedTaskCount);
}
