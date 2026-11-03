using System.Windows;

namespace ObservatorioUrnasDashboard;

public partial class ErrorDetailsWindow : Window
{
    public ErrorDetailsWindow(string title, string message, string traceback,
        string worker, string parameters)
    {
        InitializeComponent();
        ErrorTitle.Text = title;
        ErrorMessage.Text = message;
        ErrorContext.Text = string.Join(Environment.NewLine, new[]
        {
            string.IsNullOrWhiteSpace(worker) ? null : $"Worker: {worker}",
            string.IsNullOrWhiteSpace(parameters) ? null : $"Parâmetros: {parameters}",
        }.Where(line => line is not null));
        TracebackText.Text = traceback;
    }

    private void CopyDetails_Click(object sender, RoutedEventArgs e)
    {
        var details = string.Join(Environment.NewLine + Environment.NewLine,
            new[] { ErrorMessage.Text, ErrorContext.Text, TracebackText.Text }
                .Where(value => !string.IsNullOrWhiteSpace(value)));
        Clipboard.SetText(details);
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
