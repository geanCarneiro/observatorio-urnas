using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace ObservatorioUrnasDashboard.Models;

public sealed class WorkerCard : INotifyPropertyChanged
{
    private string _label = "Worker";
    private string _status = "Aguardando";
    private string _stage = "";
    private string _section = "";
    private string _progressCaption = "Progresso";
    private string _progressText = "—";
    private double _progress;
    private int? _processId;

    public string WorkerId { get; init; } = "";
    public string Label { get => _label; set => Set(ref _label, value); }
    public string Status { get => _status; set => Set(ref _status, value); }
    public string Stage { get => _stage; set => Set(ref _stage, value); }
    public string Section { get => _section; set => Set(ref _section, value); }
    public string ProgressCaption { get => _progressCaption; set => Set(ref _progressCaption, value); }
    public string ProgressText { get => _progressText; set => Set(ref _progressText, value); }
    public double Progress { get => _progress; set => Set(ref _progress, Math.Clamp(value, 0, 100)); }
    public int? ProcessId { get => _processId; set => Set(ref _processId, value); }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void Set<T>(ref T field, T value, [CallerMemberName] string? propertyName = null)
    {
        if (EqualityComparer<T>.Default.Equals(field, value)) return;
        field = value;
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}
