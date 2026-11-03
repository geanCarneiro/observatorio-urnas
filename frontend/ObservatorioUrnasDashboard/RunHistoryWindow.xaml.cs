using System.Collections.ObjectModel;
using System.Text.Json;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Media3D;
using ObservatorioUrnasDashboard.Services;

namespace ObservatorioUrnasDashboard;

public sealed class RunHistoryItem
{
    public RunSummary Summary { get; }
    public string Title => $"{Summary.Kind} · {Summary.RunId}";
    public string Status => Summary.Status;
    public RunHistoryItem(RunSummary summary) => Summary = summary;
}

public sealed record LabelValue(string Label, string Value);
public sealed record SectionHistoryItem(string Turn, string Section, string RawStatus, string ExtractStatus, string Error);
public sealed record BatchHistoryItem(string Index, string BatchId, string Status, string Rows, string Error);
public sealed record EventHistoryItem(string When, string Type, string Stage, string Section,
    string Status, string Worker, string Parameters, string Message, string Traceback, bool HasErrorDetails)
{
    public Visibility DetailsVisibility => HasErrorDetails ? Visibility.Visible : Visibility.Collapsed;
}

public partial class RunHistoryWindow : Window
{
    private const int PageSize = 50;
    private const int ActivityPageSize = 50;
    private readonly PythonUiBackend _backend;
    private readonly CancellationToken _ownerToken;
    private readonly CancellationTokenSource _windowLifetime = new();
    private readonly Dictionary<(string RunId, int Offset), string> _detailsCache = [];
    private CancellationTokenSource? _detailLoad;
    private CancellationTokenSource? _activityLoad;
    private RunHistoryItem? _selected;
    private int _offset;
    private int _sectionTotal;
    private readonly List<long?> _activityCursors = [null];
    private int _activityPageIndex;
    private bool _activityHasOlder;
    private long? _activityNextBefore;
    private string? _activityLoadedRunId;

    public NewJobConfiguration? ConfigurationToCopy { get; private set; }

    public RunHistoryWindow(PythonUiBackend backend, CancellationToken ownerToken)
    {
        InitializeComponent();
        _backend = backend;
        _ownerToken = ownerToken;
    }

    private async void Window_Loaded(object sender, RoutedEventArgs e) => await RefreshHistoryAsync();

    private async Task RefreshHistoryAsync()
    {
        try
        {
            _detailsCache.Clear();
            HistoryMessage.Text = "Carregando execuções…";
            var runs = await _backend.GetRunHistoryAsync(_ownerToken);
            RunList.ItemsSource = new ObservableCollection<RunHistoryItem>(runs.Select(run => new RunHistoryItem(run)));
            HistoryMessage.Text = runs.Count == 0 ? "Ainda não há execuções locais." : $"{runs.Count} execução(ões) listada(s).";
            if (RunList.Items.Count > 0 && RunList.SelectedItem is null)
                RunList.SelectedIndex = 0;
        }
        catch (OperationCanceledException) { }
        catch (Exception error) { HistoryMessage.Text = error.Message; }
    }

    private async void RunList_SelectionChanged(object sender, System.Windows.Controls.SelectionChangedEventArgs e)
    {
        if (RunList.SelectedItem is RunHistoryItem item)
            await SelectRunAsync(item);
    }

    private async void RunList_CurrentCellChanged(object? sender, EventArgs e)
    {
        if (RunList.CurrentCell.Item is RunHistoryItem item)
            await SelectRunAsync(item);
    }

    private async Task SelectRunAsync(RunHistoryItem item)
    {
        if (ReferenceEquals(_selected, item)) return;
        _selected = item;
        _offset = 0;
        _activityLoad?.Cancel();
        _activityCursors.Clear();
        _activityCursors.Add(null);
        _activityPageIndex = 0;
        _activityHasOlder = false;
        _activityNextBefore = null;
        _activityLoadedRunId = null;
        EventRows.ItemsSource = Array.Empty<EventHistoryItem>();
        ActivityPageInfo.Text = "Selecione esta aba para carregar as atividades.";
        UpdateActivityPagingControls();
        SelectedTitle.Text = item.Title;
        SelectedStatus.Text = $"{StatusLabel(item.Summary.Status)} · Início {FormatDate(item.Summary.CreatedAt)}";
        CopyParametersButton.IsEnabled = item.Summary.Kind is "collect" or "batch_process" or "reprocess";
        RenderImmediateSummary(item.Summary);
        await LoadDetailsAsync(item, _offset);
        if (_selected?.Summary.RunId != item.Summary.RunId) return;
        if (ActivityTab.IsSelected) await LoadActivityPageAsync();
    }

    private void RenderImmediateSummary(RunSummary run)
    {
        var info = new List<LabelValue>
        {
            new("Tipo", KindLabel(run.Kind)),
            new("Estado", StatusLabel(run.Status)),
            new("ID", run.RunId),
            new("Criado em", FormatDate(run.CreatedAt)),
            new("Concluído em", FormatDate(run.FinishedAt)),
            new("Seções geográficas", FormatNumber(run.TotalSections)),
            new("Seções e turnos", CountProperty(run.CountsJson, "total")),
            new("Remessas", FormatNumber(run.TotalBatches)),
            new("Remessas concluídas", FormatNumber(run.CompletedBatches)),
            new("Remessa atual", Display(run.CurrentBatch)),
            new("Raiz dos dados", Display(run.DataRoot)),
        };
        RunInfoRows.ItemsSource = info;
        ParameterRows.ItemsSource = ReadParameters(run.RequestJson, run.DataRoot);
        CountRows.ItemsSource = ReadCounts(run.CountsJson);
    }

    private async Task LoadDetailsAsync(RunHistoryItem item, int offset)
    {
        _detailLoad?.Cancel();
        _detailLoad?.Dispose();
        _detailLoad = CancellationTokenSource.CreateLinkedTokenSource(_ownerToken, _windowLifetime.Token);
        var cancellationToken = _detailLoad.Token;
        try
        {
            HistoryMessage.Text = "Carregando seções e eventos…";
            var cacheKey = (item.Summary.RunId, offset);
            if (!_detailsCache.TryGetValue(cacheKey, out var json))
            {
                json = await _backend.GetRunDetailsAsync(item.Summary.RunId, offset, PageSize, cancellationToken);
                if (cancellationToken.IsCancellationRequested
                    || _selected?.Summary.RunId != item.Summary.RunId) return;
                if (_detailsCache.Count > 20) _detailsCache.Clear();
                _detailsCache[cacheKey] = json;
            }
            if (cancellationToken.IsCancellationRequested
                || _selected?.Summary.RunId != item.Summary.RunId) return;
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            if (root.TryGetProperty("plan", out var plan) && plan.ValueKind == JsonValueKind.Object)
                PlanRows.ItemsSource = ReadObjectRows(plan);

            if (root.TryGetProperty("batch_states", out var batchStates) && batchStates.ValueKind == JsonValueKind.Array)
                BatchRows.ItemsSource = batchStates.EnumerateArray().Select(row => new BatchHistoryItem(
                    Value(row, "batch_index"), Value(row, "batch_id"), StatusLabel(Value(row, "status")),
                    Value(row, "row_count"), Value(row, "error"))).ToList();
            else BatchRows.ItemsSource = Array.Empty<BatchHistoryItem>();

            if (root.TryGetProperty("section_jobs", out var jobs))
            {
                _sectionTotal = IntValue(jobs, "total");
                _offset = IntValue(jobs, "offset");
                var rows = new List<SectionHistoryItem>();
                if (jobs.TryGetProperty("items", out var items) && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var row in items.EnumerateArray())
                        rows.Add(new SectionHistoryItem(Value(row, "turn"), Value(row, "section_key"),
                            StatusLabel(Value(row, "raw_status")), StatusLabel(Value(row, "extract_status")),
                            Value(row, "error")));
                }
                SectionRows.ItemsSource = rows;
                var shown = rows.Count == 0 ? "0" : $"{_offset + 1}–{_offset + rows.Count}";
                PageInfo.Text = $"Exibindo {shown} de {FormatNumber(_sectionTotal)} registros";
                PreviousPageButton.IsEnabled = _offset > 0;
                NextPageButton.IsEnabled = _offset + rows.Count < _sectionTotal;
            }
            HistoryMessage.Text = $"Detalhes de {item.Summary.RunId} carregados.";
        }
        catch (OperationCanceledException) { }
        catch (Exception error)
        {
            if (!cancellationToken.IsCancellationRequested
                && _selected?.Summary.RunId == item.Summary.RunId)
                HistoryMessage.Text = error.Message;
        }
    }

    private async void PreviousPage_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is not null) await LoadDetailsAsync(_selected, Math.Max(0, _offset - PageSize));
    }

    private async void NextPage_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is not null && _offset + PageSize < _sectionTotal)
            await LoadDetailsAsync(_selected, _offset + PageSize);
    }

    private async void DetailsTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (e.OriginalSource is TabControl && ActivityTab.IsSelected
            && _selected is not null && _activityLoadedRunId != _selected.Summary.RunId)
            await LoadActivityPageAsync();
    }

    private async Task LoadActivityPageAsync()
    {
        if (_selected is null || _activityPageIndex >= _activityCursors.Count) return;
        var selectedRunId = _selected.Summary.RunId;
        var selectedPageIndex = _activityPageIndex;
        var cursor = _activityCursors[selectedPageIndex];
        _activityLoad?.Cancel();
        _activityLoad?.Dispose();
        _activityLoad = CancellationTokenSource.CreateLinkedTokenSource(_ownerToken, _windowLifetime.Token);
        var cancellationToken = _activityLoad.Token;
        ActivityPageInfo.Text = "Carregando atividades…";
        OlderActivityButton.IsEnabled = false;
        NewerActivityButton.IsEnabled = false;
        try
        {
            var json = await _backend.GetRunEventsPageAsync(selectedRunId,
                cursor, ActivityPageSize, cancellationToken);
            if (cancellationToken.IsCancellationRequested
                || _selected?.Summary.RunId != selectedRunId
                || _activityPageIndex != selectedPageIndex) return;
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            var events = root.TryGetProperty("events", out var values)
                && values.ValueKind == JsonValueKind.Array
                ? values.EnumerateArray().Select(ToActivity).ToList()
                : [];
            EventRows.ItemsSource = events;
            _activityHasOlder = BoolValue(root, "has_older", false);
            _activityNextBefore = NullableLongValue(root, "next_before");
            _activityLoadedRunId = selectedRunId;
            ActivityPageInfo.Text = $"Página {_activityPageIndex + 1} · {events.Count} evento(s)"
                + (_activityHasOlder ? " · há atividades anteriores" : " · início do histórico");
            UpdateActivityPagingControls();
        }
        catch (OperationCanceledException) { }
        catch (Exception error)
        {
            if (!cancellationToken.IsCancellationRequested
                && _selected?.Summary.RunId == selectedRunId
                && _activityPageIndex == selectedPageIndex)
            {
                ActivityPageInfo.Text = $"Falha ao carregar atividades: {error.Message}";
                UpdateActivityPagingControls();
            }
        }
    }

    private async void OlderActivity_Click(object sender, RoutedEventArgs e)
    {
        if (_activityPageIndex + 1 < _activityCursors.Count)
            _activityPageIndex++;
        else if (_activityHasOlder && _activityNextBefore is long before)
        {
            _activityCursors.Add(before);
            _activityPageIndex++;
        }
        else return;
        await LoadActivityPageAsync();
    }

    private async void NewerActivity_Click(object sender, RoutedEventArgs e)
    {
        if (_activityPageIndex <= 0) return;
        _activityPageIndex--;
        await LoadActivityPageAsync();
    }

    private void UpdateActivityPagingControls()
    {
        NewerActivityButton.IsEnabled = _activityPageIndex > 0;
        OlderActivityButton.IsEnabled = _activityHasOlder
            || _activityPageIndex + 1 < _activityCursors.Count;
    }

    private void CopyParameters_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null) return;
        try
        {
            using var requestDocument = JsonDocument.Parse(_selected.Summary.RequestJson);
            var request = requestDocument.RootElement;
            var turn = Value(request, "turn");
            if (string.IsNullOrWhiteSpace(turn) && request.TryGetProperty("turns", out var turns)
                && turns.ValueKind == JsonValueKind.Array)
            {
                var values = turns.EnumerateArray().Select(Value).ToList();
                turn = values.Count > 1 ? "ambos" : values.FirstOrDefault() ?? "2";
            }
            if (turn is not ("1" or "2" or "ambos")) turn = "2";

            ConfigurationToCopy = new NewJobConfiguration(
                Kind: _selected.Summary.Kind,
                Turn: turn,
                Uf: Value(request, "uf"),
                Municipio: Value(request, "municipio"),
                Zona: Value(request, "zona"),
                Secao: Value(request, "secao"),
                Offset: IntValue(request, "offset"),
                Limit: NullableIntValue(request, "limit"),
                Workers: Math.Max(1, IntValue(request, "workers", 15)),
                Incremental: BoolValue(request, "incremental", true),
                SectionsPerBatch: Math.Clamp(IntValue(request, "sections_per_batch", 500), 1, 10000),
                DataRoot: _selected.Summary.DataRoot);
            DialogResult = true;
        }
        catch (Exception error) { HistoryMessage.Text = $"Não foi possível copiar os parâmetros: {error.Message}"; }
    }

    private async void Refresh_Click(object sender, RoutedEventArgs e) => await RefreshHistoryAsync();
    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowErrorDetails_Click(object sender, RoutedEventArgs e)
    {
        if ((sender as FrameworkElement)?.DataContext is not EventHistoryItem item) return;
        var details = new ErrorDetailsWindow(item.Type, item.Message, item.Traceback,
            item.Worker, item.Parameters) { Owner = this };
        details.ShowDialog();
    }

    private void SummaryScrollViewer_PreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        if (sender is not ScrollViewer parent) return;
        var inner = FindInnerScrollViewer(e.OriginalSource as DependencyObject, parent);
        if (inner is null) return;

        var canScrollInner = e.Delta > 0
            ? inner.VerticalOffset > 0
            : inner.VerticalOffset < inner.ScrollableHeight;
        if (canScrollInner) return;

        var targetOffset = Math.Clamp(parent.VerticalOffset - e.Delta / 3.0, 0, parent.ScrollableHeight);
        parent.ScrollToVerticalOffset(targetOffset);
        e.Handled = true;
    }

    private static ScrollViewer? FindInnerScrollViewer(DependencyObject? source, ScrollViewer outer)
    {
        var current = source;
        while (current is not null && !ReferenceEquals(current, outer))
        {
            if (current is ScrollViewer viewer) return viewer;
            current = current switch
            {
                ContentElement content => ContentOperations.GetParent(content)
                    ?? (content as FrameworkContentElement)?.Parent,
                Visual or Visual3D => VisualTreeHelper.GetParent(current),
                _ => LogicalTreeHelper.GetParent(current),
            };
        }
        return null;
    }

    protected override void OnClosed(EventArgs e)
    {
        _windowLifetime.Cancel();
        _detailLoad?.Cancel();
        _detailLoad?.Dispose();
        _activityLoad?.Cancel();
        _activityLoad?.Dispose();
        _windowLifetime.Dispose();
        base.OnClosed(e);
    }

    private static List<LabelValue> ReadParameters(string json, string dataRoot)
    {
        using var document = JsonDocument.Parse(json);
        var request = document.RootElement;
        var rows = new List<LabelValue>();
        var turn = Value(request, "turn");
        if (string.IsNullOrWhiteSpace(turn) && request.TryGetProperty("turns", out var turns)
            && turns.ValueKind == JsonValueKind.Array)
            turn = string.Join(", ", turns.EnumerateArray().Select(Value));
        Add("Turno(s)", turn is "1" or "2" or "ambos" ? TurnLabel(turn) : turn);
        Add("UF", Value(request, "uf"));
        Add("Município · código TSE", Value(request, "municipio"));
        Add("Zona", Value(request, "zona"));
        Add("Seção", Value(request, "secao"));
        Add("Offset", Value(request, "offset"));
        Add("Limite de seções", request.TryGetProperty("limit", out var limit) && limit.ValueKind != JsonValueKind.Null
            ? limit.ToString() : "Sem limite");
        Add("Workers", Value(request, "workers"));
        Add("Modo", BoolValue(request, "incremental", true) ? "Incremental" : "Atualização dos dados brutos");
        Add("Seções por remessa", Value(request, "sections_per_batch"));
        Add("Raiz dos dados", Value(request, "data_root") is { Length: > 0 } requestRoot ? requestRoot : dataRoot);
        return rows;

        void Add(string label, string value)
        {
            if (!string.IsNullOrWhiteSpace(value)) rows.Add(new LabelValue(label, value));
        }
    }

    private static List<LabelValue> ReadCounts(string json)
    {
        using var document = JsonDocument.Parse(json);
        var labels = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["total"] = "Total de posições", ["collected"] = "Coletadas",
            ["skipped_existing"] = "Ignoradas por já existirem", ["aggregated"] = "Agregadas",
            ["partial"] = "Parciais", ["failed"] = "Falhas", ["pending"] = "Pendentes",
            ["extraction_failed"] = "Falhas na extração", ["extraction_pending"] = "Extração pendente",
            ["extraction_complete"] = "Extração concluída",
        };
        if (document.RootElement.ValueKind != JsonValueKind.Object) return [];
        return document.RootElement.EnumerateObject()
            .Where(property => property.Value.ValueKind == JsonValueKind.Number)
            .Select(property => new LabelValue(labels.GetValueOrDefault(property.Name, Humanize(property.Name)),
                FormatNumber(property.Value.GetInt64())))
            .ToList();
    }

    private static List<LabelValue> ReadObjectRows(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object) return [];
        return element.EnumerateObject()
            .Where(property => property.Value.ValueKind is JsonValueKind.String or JsonValueKind.Number
                or JsonValueKind.True or JsonValueKind.False or JsonValueKind.Array)
            .Select(property => new LabelValue(Humanize(property.Name), FormatValue(property.Value)))
            .Where(row => !string.IsNullOrWhiteSpace(row.Value))
            .ToList();
    }

    private static EventHistoryItem ToActivity(JsonElement element)
    {
        var message = Value(element, "message");
        var error = Value(element, "error");
        if (string.IsNullOrWhiteSpace(message)) message = error;
        var worker = Value(element, "worker_id");
        if (string.IsNullOrWhiteSpace(worker) && Value(element, "process_id") is { Length: > 0 } processId)
            worker = $"PID {processId}";
        var parameters = element.TryGetProperty("worker_parameters", out var workerParameters)
            && workerParameters.ValueKind == JsonValueKind.Object
            ? workerParameters.GetRawText() : "";
        var eventType = Value(element, "type");
        var eventStatus = Value(element, "status");
        if (eventType == "stage_complete" && !string.IsNullOrWhiteSpace(error))
            eventStatus = "failed";
        var typeLabel = eventType == "stage_complete"
            ? eventStatus == "failed" ? "Etapa falhou" : "Etapa concluída"
            : Humanize(eventType);
        return new EventHistoryItem(FormatDate(Value(element, "recorded_at_utc")),
            typeLabel, Value(element, "stage"),
            FormatSection(Value(element, "section"), Value(element, "turn")),
            StatusLabel(eventStatus), worker, parameters, message,
            Value(element, "traceback"), !string.IsNullOrWhiteSpace(error)
                || !string.IsNullOrWhiteSpace(Value(element, "traceback")));
    }

    private static string FormatSection(string section, string turn) =>
        string.IsNullOrWhiteSpace(section) ? "" : string.IsNullOrWhiteSpace(turn) ? section : $"{section} · turno {turn}";
    private static string TurnLabel(string value) => value switch
    {
        "1" => "1º turno", "2" => "2º turno", "ambos" => "Ambos os turnos", _ => value,
    };
    private static string KindLabel(string value) => value switch
    {
        "collect" => "Coleta", "batch_process" => "Coleta e conciliação por remessas",
        "reprocess" => "Reprocessamento local", _ => Humanize(value),
    };
    private static string StatusLabel(string value) => value switch
    {
        "complete" or "completed" or "collected" => "Concluído",
        "running" => "Em andamento", "preparing" => "Preparando", "paused" => "Pausado",
        "partial" => "Parcial", "failed" or "error" => "Falha", "pending" => "Pendente",
        "skipped_existing" => "Já existia", "aggregated" => "Agregada", "not_needed" => "Não necessária",
        _ => Humanize(value),
    };
    private static string Humanize(string value) => string.IsNullOrWhiteSpace(value)
        ? "—" : string.Join(" ", value.Split('_', StringSplitOptions.RemoveEmptyEntries)
            .Select(word => char.ToUpperInvariant(word[0]) + word[1..]));
    private static string FormatValue(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Array => string.Join(", ", value.EnumerateArray().Select(Value)),
        JsonValueKind.True => "Sim", JsonValueKind.False => "Não", JsonValueKind.Null => "—",
        _ => value.ToString(),
    };
    private static string FormatDate(string value) => DateTimeOffset.TryParse(value, out var date)
        ? date.ToLocalTime().ToString("g") : Display(value);
    private static string FormatNumber(long value) => value.ToString("N0", System.Globalization.CultureInfo.GetCultureInfo("pt-BR"));
    private static string CountProperty(string json, string property)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.TryGetProperty(property, out var value) && value.TryGetInt64(out var count)
            ? FormatNumber(count) : "—";
    }
    private static string Display(string value) => string.IsNullOrWhiteSpace(value) ? "—" : value;
    private static string Value(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value)
            ? FormatValue(value) : "";
    private static string Value(JsonElement element) => FormatValue(element);
    private static int IntValue(JsonElement element, string property, int fallback = 0) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value)
        && value.TryGetInt32(out var result) ? result : fallback;
    private static int? NullableIntValue(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value)
        && value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var result) ? result : null;
    private static long? NullableLongValue(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value)
        && value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var result) ? result : null;
    private static bool BoolValue(JsonElement element, string property, bool fallback) =>
        element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property, out var value)
        && value.ValueKind is JsonValueKind.True or JsonValueKind.False ? value.GetBoolean() : fallback;
}
