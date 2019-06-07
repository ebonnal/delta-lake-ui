import pygal

fig_pygal="""
<figure>
  <!-- Pygal render() result: -->
  {a}
  <!-- End of Pygal render() result: -->
</figure>
"""

def to_html_fig(chart):
    plot_html_fig = fig_pygal.format(a=chart.render(is_unicode=True))
    return plot_html_fig

def plot(data, statement):
    line_chart = pygal.Line(fill=True)
    line_chart.title = f'Result evolution among Delta Lake versions\n{statement}'
    line_chart.x_labels = [d[0] for d in data]
    line_chart.add(None, [d[1] for d in data])
    return to_html_fig(line_chart)
