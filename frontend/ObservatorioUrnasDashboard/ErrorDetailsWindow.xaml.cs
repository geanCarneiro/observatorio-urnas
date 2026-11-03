using System.Windows;

namespace ObservatorioUrnasDashboard;

public partial class ErrorDetailsWindow : Window
{
    public ErrorDetailsWindow(string title, string message, string traceback)
    {
        InitializeComponent();
        ErrorTitle.Text = title;
        ErrorMessage.Text = message;
        TracebackText.Text = traceback;
    }

    private void CopyDetails_Click(object sender, RoutedEventArgs e)
    {
        var details = string.IsNullOrWhiteSpace(TracebackText.Text)
            ? ErrorMessage.Text
            : $"{ErrorMessage.Text}{Environment.NewLine}{Environment.NewLine}{TracebackText.Text}";
        Clipboard.SetText(details);
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
