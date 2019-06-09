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


previous = []

def plot(data, statement, append=False):
    if type(data) is str:
        return f"ERROR OCCURRED: {data}"
    global previous
    line_chart = pygal.Line(fill=True, truncate_legend=7)
    line_chart.x_labels = [d[0] for d in data]
    try:

        if append:
            previous.append((statement, data))
            for statement, data in previous:
                line_chart.add(statement, list(map(lambda y: int(y) if y is not None else None, [d[1] for d in data])))
        else:
            line_chart.title = f'Result evolution among Delta Lake versions\n{statement}'
            line_chart.add(None, list(map(lambda y: int(y) if y is not None else None, [d[1] for d in data])))
            previous = []
            previous.append((statement, data))

        return to_html_fig(line_chart)
    except Exception as e:
        return f"ERROR OCCURRED: {str(e)}"


html_pygal="""
<!DOCTYPE html>
<html>
  <head>
  <script type="text/javascript" src="http://kozea.github.com/pygal.js/latest/pygal-tooltips.min.js"></script>
    <!-- ... -->
  </head>
  <body>
    <figure>
      <!-- Pygal render() result: -->
      {a}
      <!-- End of Pygal render() result: -->
    </figure>
  </body>
</html>
"""
