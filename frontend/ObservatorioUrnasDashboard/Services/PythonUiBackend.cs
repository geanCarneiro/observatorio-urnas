using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace ObservatorioUrnasDashboard.Services;

public sealed record RunView(string RunId, string Kind, string Status, int WorkerLimit,
    bool CanResume, bool CanStop, string? Error);

public sealed record DashboardSnapshot(string? RunId, string RunTitle, string RunStatus,
    int WorkerLimit, double? Progress, string Stage, string StatusMessage,
    bool CanResume, bool CanStop, bool CanCreateJob, string ActivityText);

public sealed record RunSummary(string RunId, string Kind, string Status,
    string CreatedAt, string FinishedAt, string DataRoot, string RequestJson,
    string CountsJson, int TotalSections, int TotalBatches, int CompletedBatches,
    string CurrentBatch);

public sealed record JobPreview(string PreviewDirectory, string Summary);

public sealed record NewJobConfiguration(string Kind = "collect", string Turn = "2",
    string Uf = "", string Municipio = "", string Zona = "", string Secao = "",
    int Offset = 0, int? Limit = null, int Workers = 15, bool Incremental = true,
    int SectionsPerBatch = 500, string? DataRoot = null);

public sealed class PythonUiBackend : IDisposable
{
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(30) };
    private Process? _server;
    private Uri? _baseUri;
    private readonly StringBuilder _stderr = new();

    public Uri? BaseUri => _baseUri;

    public async Task StartAsync(string repositoryRoot, CancellationToken cancellationToken)
    {
        var script = Path.Combine(repositoryRoot, "tools", "local_ui.py");
        if (!File.Exists(script)) throw new FileNotFoundException("Não encontrei tools/local_ui.py.", script);

        var startInfo = new ProcessStartInfo
        {
            FileName = "python",
            WorkingDirectory = repositoryRoot,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };
        startInfo.ArgumentList.Add(script);
        startInfo.ArgumentList.Add("--no-open");
        startInfo.Environment["PYTHONIOENCODING"] = "utf-8";

        _server = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        if (!_server.Start()) throw new InvalidOperationException("Não consegui iniciar o servidor local Python.");
        _ = CaptureErrorsAsync(_server.StandardError);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        while (true)
        {
            var line = await _server.StandardOutput.ReadLineAsync(timeout.Token);
            if (line is null)
            {
                var error = await ReadErrorAsync();
                throw new InvalidOperationException(string.IsNullOrWhiteSpace(error)
                    ? "O servidor local Python encerrou antes de ficar pronto."
                    : $"O servidor local Python encerrou: {error}");
            }
            var match = Regex.Match(line, "https?://127\\.0\\.0\\.1:\\d+");
            if (!match.Success) continue;
            _baseUri = new Uri(match.Value);
            _http.BaseAddress = _baseUri;
            return;
        }
    }

    public async Task<DashboardSnapshot> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        using var response = await _http.GetAsync("/api/state", cancellationToken);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var root = document.RootElement;
        var busy = root.TryGetProperty("busy", out var busyValue) && busyValue.ValueKind == JsonValueKind.True;
        var runs = root.TryGetProperty("runs", out var runsValue) && runsValue.ValueKind == JsonValueKind.Array
            ? runsValue.EnumerateArray().ToList() : [];
        var activeRunId = root.TryGetProperty("attached_run", out var attached)
            && attached.ValueKind == JsonValueKind.Object && attached.TryGetProperty("run_id", out var attachedId)
            ? attachedId.GetString() : null;
        JsonElement? run = null;
        if (activeRunId is not null)
        {
            foreach (var item in runs)
            {
                if (item.TryGetProperty("run_id", out var id) && id.GetString() == activeRunId)
                {
                    run = item;
                    break;
                }
            }
        }
        if (run is null && runs.Count > 0) run = runs[0];

        if (run is null)
        {
            return new DashboardSnapshot(null, "Nenhum job local", "Sem execução", 0,
                null, "Aguardando uma execução local.", "Aguardando uma execução local.",
                false, false, !busy, ReadRecentActivity(root));
        }

        var runRecord = run.Value;
        var runId = GetString(runRecord, "run_id") ?? "";
        var runStatus = GetString(runRecord, "status") ?? "desconhecido";
        var kind = GetString(runRecord, "kind") ?? "job";
        var workerLimit = 15;
        if (runRecord.TryGetProperty("request", out var request) && request.ValueKind == JsonValueKind.Object
            && request.TryGetProperty("workers", out var workers) && workers.TryGetInt32(out var configuredWorkers))
            workerLimit = Math.Max(1, configuredWorkers);
        else if (root.TryGetProperty("jobs", out var jobs) && jobs.TryGetProperty("worker_limit", out var workerLimitValue)
                 && workerLimitValue.TryGetInt32(out var snapshotWorkers))
            workerLimit = Math.Max(1, snapshotWorkers);

        var detail = root.TryGetProperty("progress_detail", out var progressDetail)
            && progressDetail.ValueKind == JsonValueKind.Object ? progressDetail : default;
        var stage = detail.ValueKind == JsonValueKind.Object ? GetString(detail, "stage") : null;
        if (runStatus == "paused" && stage?.StartsWith("Sem atualização observável", StringComparison.OrdinalIgnoreCase) == true)
            stage = "Execução pausada";
        var progress = root.TryGetProperty("progress", out var progressValue)
            && progressValue.ValueKind == JsonValueKind.Number ? progressValue.GetDouble() : (double?)null;
        var canStop = root.TryGetProperty("can_stop", out var canStopValue)
            && canStopValue.ValueKind == JsonValueKind.True && busy;
        var canResume = !busy && runStatus is "paused" or "partial" or "failed";
        var message = GetString(runRecord, "error");
        if (string.IsNullOrWhiteSpace(message))
            message = busy ? "Execução ativa." : runStatus switch
            {
                "paused" => "A execução está pausada e pode ser retomada.",
                "complete" => "A execução mais recente foi concluída.",
                "partial" => "A execução terminou com itens pendentes ou falhos.",
                "failed" => "A execução mais recente falhou.",
                _ => "Estado local carregado.",
            };

        return new DashboardSnapshot(runId, $"{kind} · {runId}", runStatus, workerLimit,
            progress, stage ?? runStatus, message, canResume, canStop,
            !busy && runStatus is not ("running" or "preparing"), ReadRecentActivity(root));
    }

    public async Task<IReadOnlyList<RunSummary>> GetRunHistoryAsync(CancellationToken cancellationToken)
    {
        using var response = await _http.GetAsync("/api/state", cancellationToken);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        if (!document.RootElement.TryGetProperty("runs", out var runs) || runs.ValueKind != JsonValueKind.Array)
            return [];
        return runs.EnumerateArray().Select(run => new RunSummary(
            GetString(run, "run_id") ?? "",
            GetString(run, "kind") ?? "job",
            GetString(run, "status") ?? "desconhecido",
            GetString(run, "created_at_utc") ?? "",
            GetString(run, "finished_at_utc") ?? "",
            GetString(run, "data_root") ?? "",
            run.TryGetProperty("request", out var request) ? request.GetRawText() : "{}",
            run.TryGetProperty("counts", out var counts) ? counts.GetRawText() : "{}",
            GetInt(run, "total_sections"), GetInt(run, "total_batches"),
            GetInt(run, "completed_batches"), GetString(run, "current_batch") ?? ""))
            .Where(run => run.RunId.Length > 0).ToList();
    }

    public async Task<string> GetRunDetailsAsync(string runId, int offset, int limit,
        CancellationToken cancellationToken)
    {
        var path = $"/api/runs/{Uri.EscapeDataString(runId)}?offset={offset}&limit={limit}&compact=1";
        using var response = await _http.GetAsync(path, cancellationToken);
        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode) throw ApiError(content, response.ReasonPhrase);
        return content;
    }

    public async Task<string> GetRunEventsPageAsync(string runId, long? before, int limit,
        CancellationToken cancellationToken)
    {
        var path = $"/api/runs/{Uri.EscapeDataString(runId)}/events?limit={limit}";
        if (before is not null) path += $"&before={before.Value}";
        using var response = await _http.GetAsync(path, cancellationToken);
        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode) throw ApiError(content, response.ReasonPhrase);
        return content;
    }

    public async Task<JobPreview> PreviewAsync(NewJobConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var before = await GetStateAsync(null, cancellationToken);
        var cursor = before.RootElement.TryGetProperty("cursor", out var cursorValue) && cursorValue.TryGetInt64(out var parsedCursor)
            ? parsedCursor : 0;
        await PostAsync("/api/preview", SelectionPayload(configuration), cancellationToken);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var state = await GetStateAsync(cursor, cancellationToken);
            if (state.RootElement.TryGetProperty("events", out var events) && events.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in events.EnumerateArray())
                {
                    if (item.TryGetProperty("cursor", out var eventCursor) && eventCursor.TryGetInt64(out var nextCursor))
                        cursor = Math.Max(cursor, nextCursor);
                    var type = GetString(item, "type");
                    if (type == "preview_failed")
                        throw new InvalidOperationException(GetString(item, "error") ?? "Falha ao calcular a prévia.");
                    if (type == "preview_ready" && item.TryGetProperty("result", out var result))
                        return ParsePreview(result);
                }
            }
            await Task.Delay(350, cancellationToken);
        }
    }

    public Task StartCollectionAsync(NewJobConfiguration configuration, JobPreview preview,
        CancellationToken cancellationToken)
    {
        var payload = SelectionPayload(configuration);
        payload["preview_dir"] = preview.PreviewDirectory;
        payload["incremental"] = configuration.Incremental;
        return PostAsync("/api/collect", payload, cancellationToken);
    }

    public Task StartBatchProcessAsync(NewJobConfiguration configuration, JobPreview preview,
        CancellationToken cancellationToken)
    {
        var payload = SelectionPayload(configuration);
        payload["preview_dir"] = preview.PreviewDirectory;
        payload["sections_per_batch"] = configuration.SectionsPerBatch;
        payload["confirm_processing"] = true;
        return PostAsync("/api/batch-process", payload, cancellationToken);
    }

    public Task StartReprocessAsync(NewJobConfiguration configuration, CancellationToken cancellationToken) =>
        PostAsync("/api/reprocess", new Dictionary<string, object?>
        {
            ["workers"] = configuration.Workers,
            ["data_root"] = configuration.DataRoot,
        }, cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => PostAsync("/api/stop", "{}", cancellationToken);

    public Task ResumeAsync(string runId, CancellationToken cancellationToken) =>
        PostAsync("/api/resume", JsonSerializer.Serialize(new { run_id = runId }), cancellationToken);

    private async Task PostAsync(string path, string json, CancellationToken cancellationToken)
    {
        using var body = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await _http.PostAsync(path, body, cancellationToken);
        var responseText = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            try
            {
                using var error = JsonDocument.Parse(responseText);
                throw ApiError(responseText, response.ReasonPhrase);
            }
            catch (JsonException)
            {
                throw new InvalidOperationException(responseText.Length > 0 ? responseText : response.ReasonPhrase);
            }
        }
    }

    private async Task PostAsync(string path, object payload, CancellationToken cancellationToken)
    {
        using var body = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
        using var response = await _http.PostAsync(path, body, cancellationToken);
        var responseText = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode) throw ApiError(responseText, response.ReasonPhrase);
    }

    private async Task<JsonDocument> GetStateAsync(long? since, CancellationToken cancellationToken)
    {
        var path = since is null ? "/api/state" : $"/api/state?since={since.Value}";
        using var response = await _http.GetAsync(path, cancellationToken);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        return await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
    }

    private static Dictionary<string, object?> SelectionPayload(NewJobConfiguration configuration) => new()
    {
        ["turn"] = configuration.Turn,
        ["uf"] = configuration.Uf,
        ["municipio"] = configuration.Municipio,
        ["zona"] = configuration.Zona,
        ["secao"] = configuration.Secao,
        ["offset"] = configuration.Offset,
        ["limit"] = configuration.Limit,
        ["workers"] = configuration.Workers,
    };

    private static JobPreview ParsePreview(JsonElement result)
    {
        var lines = new List<string>();
        if (result.TryGetProperty("turns", out var turns) && turns.ValueKind == JsonValueKind.Array)
        {
            foreach (var turn in turns.EnumerateArray())
            {
                var turnLabel = turn.TryGetProperty("turn", out var turnValue)
                    ? turnValue.ValueKind == JsonValueKind.Number
                        ? turnValue.GetRawText()
                        : turnValue.ValueKind == JsonValueKind.String ? turnValue.GetString() ?? "?" : "?"
                    : "?";
                lines.Add($"Turno {turnLabel}: {NumberString(turn, "selected_sections")} selecionadas, {NumberString(turn, "collectable_sections")} coletáveis, {NumberString(turn, "aggregated_sections")} agregadas");
            }
        }
        lines.Add($"Total selecionado: {NumberString(result, "total_selected_sections")} seções");
        return new JobPreview(GetString(result, "preview_dir") ?? "", string.Join(Environment.NewLine, lines));
    }

    private static string NumberString(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.Number
            ? value.GetRawText() : "0";

    private static InvalidOperationException ApiError(string responseText, string? fallback)
    {
        try
        {
            using var error = JsonDocument.Parse(responseText);
            return new InvalidOperationException(GetString(error.RootElement, "error") ?? fallback ?? "Falha na API local.");
        }
        catch (JsonException)
        {
            return new InvalidOperationException(responseText.Length > 0 ? responseText : fallback ?? "Falha na API local.");
        }
    }

    private async Task CaptureErrorsAsync(StreamReader reader)
    {
        try
        {
            while (await reader.ReadLineAsync() is { } line)
            {
                lock (_stderr)
                {
                    _stderr.AppendLine(line);
                    if (_stderr.Length > 12000) _stderr.Remove(0, _stderr.Length - 12000);
                }
            }
        }
        catch (ObjectDisposedException) { }
    }

    private Task<string> ReadErrorAsync()
    {
        lock (_stderr) return Task.FromResult(_stderr.ToString().Trim());
    }

    private static string ReadRecentActivity(JsonElement root)
    {
        if (!root.TryGetProperty("recent_events", out var events) || events.ValueKind != JsonValueKind.Array)
            return "Sem atividade recente.";
        var lines = new List<string>();
        foreach (var item in events.EnumerateArray().TakeLast(12))
        {
            var message = GetString(item, "message");
            if (string.IsNullOrWhiteSpace(message))
            {
                var type = GetString(item, "type") ?? "evento";
                var stage = GetString(item, "stage");
                var section = GetString(item, "section");
                message = string.Join(" · ", new[] { type, stage, section }.Where(value => !string.IsNullOrWhiteSpace(value)));
            }
            if (!string.IsNullOrWhiteSpace(message)) lines.Add(message);
        }
        return lines.Count == 0 ? "Sem atividade recente." : string.Join(Environment.NewLine, lines);
    }

    private static string? GetString(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() : null;

    private static int GetInt(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.TryGetInt32(out var result) ? result : 0;

    public void Dispose()
    {
        _http.Dispose();
        if (_server is null) return;
        try
        {
            if (!_server.HasExited) _server.Kill();
            _server.Dispose();
        }
        catch (InvalidOperationException) { }
    }
}
