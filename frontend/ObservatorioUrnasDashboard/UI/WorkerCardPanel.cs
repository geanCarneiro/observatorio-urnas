using System.Windows;
using System.Windows.Controls;

namespace ObservatorioUrnasDashboard.UI;

/// <summary>Wraps cards at a minimum width and stretches the final row to fill the panel.</summary>
public sealed class WorkerCardPanel : Panel
{
    public static readonly DependencyProperty MinimumCardWidthProperty = DependencyProperty.Register(
        nameof(MinimumCardWidth), typeof(double), typeof(WorkerCardPanel), new FrameworkPropertyMetadata(220d,
            FrameworkPropertyMetadataOptions.AffectsMeasure | FrameworkPropertyMetadataOptions.AffectsArrange));

    public static readonly DependencyProperty SpacingProperty = DependencyProperty.Register(
        nameof(Spacing), typeof(double), typeof(WorkerCardPanel), new FrameworkPropertyMetadata(10d,
            FrameworkPropertyMetadataOptions.AffectsMeasure | FrameworkPropertyMetadataOptions.AffectsArrange));

    public double MinimumCardWidth
    {
        get => (double)GetValue(MinimumCardWidthProperty);
        set => SetValue(MinimumCardWidthProperty, value);
    }

    public double Spacing
    {
        get => (double)GetValue(SpacingProperty);
        set => SetValue(SpacingProperty, value);
    }

    protected override Size MeasureOverride(Size availableSize)
    {
        if (InternalChildren.Count == 0) return new Size(0, 0);
        var width = double.IsInfinity(availableSize.Width) ? MinimumCardWidth * 4 : availableSize.Width;
        var columns = GetColumnCount(width);
        var rows = (InternalChildren.Count + columns - 1) / columns;
        var totalHeight = 0d;
        for (var row = 0; row < rows; row++)
        {
            var count = Math.Min(columns, InternalChildren.Count - row * columns);
            var cardWidth = Math.Max(1, (width - Spacing * (count - 1)) / count);
            var rowHeight = 0d;
            for (var column = 0; column < count; column++)
            {
                var child = InternalChildren[row * columns + column];
                child.Measure(new Size(cardWidth, double.PositiveInfinity));
                rowHeight = Math.Max(rowHeight, child.DesiredSize.Height);
            }
            totalHeight += rowHeight + (row == rows - 1 ? 0 : Spacing);
        }
        return new Size(width, totalHeight);
    }

    protected override Size ArrangeOverride(Size finalSize)
    {
        if (InternalChildren.Count == 0) return finalSize;
        var columns = GetColumnCount(finalSize.Width);
        var rows = (InternalChildren.Count + columns - 1) / columns;
        var rowHeights = new double[rows];
        for (var index = 0; index < InternalChildren.Count; index++)
        {
            var row = index / columns;
            rowHeights[row] = Math.Max(rowHeights[row], InternalChildren[index].DesiredSize.Height);
        }

        var y = 0d;
        for (var row = 0; row < rows; row++)
        {
            var count = Math.Min(columns, InternalChildren.Count - row * columns);
            var cardWidth = Math.Max(1, (finalSize.Width - Spacing * (count - 1)) / count);
            for (var column = 0; column < count; column++)
            {
                var x = column * (cardWidth + Spacing);
                InternalChildren[row * columns + column].Arrange(new Rect(x, y, cardWidth, rowHeights[row]));
            }
            y += rowHeights[row] + Spacing;
        }
        return finalSize;
    }

    private int GetColumnCount(double width)
    {
        var cardWidth = Math.Max(1, MinimumCardWidth);
        return Math.Max(1, (int)Math.Floor((Math.Max(1, width) + Spacing) / (cardWidth + Spacing)));
    }
}
