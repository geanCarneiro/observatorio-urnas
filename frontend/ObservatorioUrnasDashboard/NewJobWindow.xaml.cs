using System.ComponentModel;
using System.Windows;
using System.Windows.Controls;
using ObservatorioUrnasDashboard.Services;

namespace ObservatorioUrnasDashboard;

public partial class NewJobWindow : Window
{
    private readonly PythonUiBackend _backend;
    private readonly CancellationToken _ownerToken;
    private JobPreview? _preview;
    private bool _previewing;
    private bool _starting;
    private bool _initializing;

    public NewJobWindow(PythonUiBackend backend, NewJobConfiguration? initial,
        CancellationToken ownerToken)
    {
        InitializeComponent();
        _backend = backend;
        _ownerToken = ownerToken;
        if (initial is not null) ApplyConfiguration(initial);
        UpdateMode();
    }

    private void Window_Loaded(object sender, RoutedEventArgs e) => UpdateMode();

    private void ApplyConfiguration(NewJobConfiguration configuration)
    {
        _initializing = true;
        ModeSelector.SelectedValue = configuration.Kind is "batch_process" or "reprocess"
            ? configuration.Kind : "collect";
        TurnSelector.SelectedValue = configuration.Turn is "1" or "2" or "ambos" ? configuration.Turn : "2";
        UfBox.Text = configuration.Uf;
        MunicipioBox.Text = configuration.Municipio;
        ZonaBox.Text = configuration.Zona;
        SecaoBox.Text = configuration.Secao;
        OffsetBox.Text = configuration.Offset.ToString();
        LimitBox.Text = configuration.Limit?.ToString() ?? "";
        WorkersBox.Text = configuration.Workers.ToString();
        IncrementalBox.IsChecked = configuration.Incremental;
        BatchSizeBox.Text = configuration.SectionsPerBatch.ToString();
        DataRootBox.Text = configuration.DataRoot ?? "";
        _initializing = false;
    }

    private void Mode_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_initializing || !IsLoaded) return;
        InvalidatePreview();
        UpdateMode();
    }

    private void Selection_Changed(object sender, RoutedEventArgs e)
    {
        if (_initializing || !IsLoaded) return;
        InvalidatePreview();
    }

    private void UpdateMode()
    {
        var mode = SelectedMode;
        var reprocess = mode == "reprocess";
        var batch = mode == "batch_process";
        SelectionPanel.Visibility = reprocess ? Visibility.Collapsed : Visibility.Visible;
        CollectionOptions.Visibility = reprocess ? Visibility.Collapsed : Visibility.Visible;
        IncrementalBox.Visibility = reprocess || batch ? Visibility.Collapsed : Visibility.Visible;
        BatchOptions.Visibility = batch ? Visibility.Visible : Visibility.Collapsed;
        ReprocessOptions.Visibility = reprocess ? Visibility.Visible : Visibility.Collapsed;
        PreviewButton.Visibility = reprocess ? Visibility.Collapsed : Visibility.Visible;
        ModeSelector.IsEnabled = !_previewing;
        SelectionPanel.IsEnabled = !_previewing;
        PreviewButton.IsEnabled = !_previewing && !reprocess;
        ModeDescription.Text = mode switch
        {
            "batch_process" => "Reutiliza seções já consolidadas, verifica sessões pendentes com dados ou estado incerto, coleta e concilia por remessas. Os dados brutos são limpos por seção após consolidação segura.",
            "reprocess" => "Reprocessa aquisições locais já existentes. Não baixa dados brutos.",
            _ => "Por padrão, ignora arquivos já existentes. Desmarque o modo incremental para atualizar os dados brutos das seções selecionadas.",
        };
        StartButton.IsEnabled = !_previewing && !_starting && (reprocess || _preview is not null);
    }

    private async void Preview_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            ErrorText.Text = "";
            var configuration = ReadConfiguration();
            _previewing = true;
            UpdateMode();
            PreviewProgress.Visibility = Visibility.Visible;
            PreviewText.Text = "Consultando inventário e calculando a seleção…";
            var preview = await _backend.PreviewAsync(configuration, _ownerToken);
            _preview = preview;
            PreviewText.Text = preview.Summary;
        }
        catch (Exception error) { ErrorText.Text = error.Message; }
        finally
        {
            _previewing = false;
            PreviewProgress.Visibility = Visibility.Collapsed;
            UpdateMode();
        }
    }

    private async void Start_Click(object sender, RoutedEventArgs e)
    {
        if (_starting) return;
        _starting = true;
        UpdateMode();
        try
        {
            ErrorText.Text = "";
            var configuration = ReadConfiguration();
            if (SelectedMode == "reprocess")
            {
                var answer = MessageBox.Show(this,
                    $"Iniciar reprocessamento local com {configuration.Workers} workers? Nenhum arquivo bruto será baixado.{Environment.NewLine}{(configuration.DataRoot is null ? "" : $"Raiz de dados: {configuration.DataRoot}")}",
                    "Confirmar reprocessamento", MessageBoxButton.YesNo, MessageBoxImage.Question, MessageBoxResult.No);
                if (answer != MessageBoxResult.Yes) return;
                await _backend.StartReprocessAsync(configuration, _ownerToken);
            }
            else if (SelectedMode == "batch_process")
            {
                if (_preview is null) return;
                var answer = MessageBox.Show(this,
                    $"Iniciar o processamento por remessas para a prévia abaixo?{Environment.NewLine}{Environment.NewLine}{_preview.Summary}{Environment.NewLine}{Environment.NewLine}O job poderá reutilizar seções já consolidadas. A auditoria física será limitada a sessões pendentes com dados existentes ou estado incerto; os dados brutos serão limpos por seção após a consolidação segura.",
                    "Confirmar processamento", MessageBoxButton.YesNo, MessageBoxImage.Question, MessageBoxResult.No);
                if (answer != MessageBoxResult.Yes) return;
                await _backend.StartBatchProcessAsync(configuration, _preview, _ownerToken);
            }
            else
            {
                if (_preview is null) return;
                var acquisitionMode = configuration.Incremental ? "incremental" : "atualização dos dados brutos";
                var answer = MessageBox.Show(this,
                    $"Iniciar coleta em modo {acquisitionMode}?{Environment.NewLine}{Environment.NewLine}{_preview.Summary}{Environment.NewLine}{Environment.NewLine}Nenhum arquivo de dados/ será removido.",
                    "Confirmar coleta", MessageBoxButton.YesNo, MessageBoxImage.Question, MessageBoxResult.No);
                if (answer != MessageBoxResult.Yes) return;
                await _backend.StartCollectionAsync(configuration, _preview, _ownerToken);
            }
            _starting = false;
            DialogResult = true;
        }
        catch (Exception error) { ErrorText.Text = error.Message; }
        finally
        {
            _starting = false;
            UpdateMode();
        }
    }

    private NewJobConfiguration ReadConfiguration()
    {
        var workers = ParseInt(WorkersBox.Text, "workers", minimum: 1, maximum: 61);
        var mode = SelectedMode;
        if (mode == "reprocess")
            return new NewJobConfiguration(Kind: mode, Workers: workers,
                DataRoot: string.IsNullOrWhiteSpace(DataRootBox.Text) ? null : DataRootBox.Text.Trim());

        var offset = ParseInt(OffsetBox.Text, "offset", minimum: 0, maximum: int.MaxValue);
        int? limit = string.IsNullOrWhiteSpace(LimitBox.Text) ? null
            : ParseInt(LimitBox.Text, "máximo de seções", minimum: 1, maximum: int.MaxValue);
        var batchSize = mode == "batch_process"
            ? ParseInt(BatchSizeBox.Text, "seções por remessa", minimum: 1, maximum: 10000) : 500;
        return new NewJobConfiguration(Kind: mode,
            Turn: TurnSelector.SelectedValue?.ToString() ?? "2",
            Uf: UfBox.Text.Trim(), Municipio: MunicipioBox.Text.Trim(), Zona: ZonaBox.Text.Trim(),
            Secao: SecaoBox.Text.Trim(), Offset: offset, Limit: limit, Workers: workers,
            Incremental: IncrementalBox.IsChecked == true, SectionsPerBatch: batchSize);
    }

    private string SelectedMode => ModeSelector.SelectedValue?.ToString() ?? "collect";

    private static int ParseInt(string text, string label, int minimum, int maximum)
    {
        if (!int.TryParse(text, out var value) || value < minimum || value > maximum)
            throw new InvalidOperationException($"{label} deve estar entre {minimum} e {maximum}.");
        return value;
    }

    private void InvalidatePreview()
    {
        if (_preview is null) return;
        _preview = null;
        PreviewText.Text = "Os parâmetros mudaram. Calcule uma nova prévia antes de iniciar.";
        UpdateMode();
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => Close();

    private void Window_Closing(object? sender, CancelEventArgs e)
    {
        if (_previewing || _starting)
        {
            e.Cancel = true;
            ErrorText.Text = _previewing
                ? "Aguarde o cálculo da prévia terminar."
                : "Aguarde o início do job terminar.";
        }
    }
}
