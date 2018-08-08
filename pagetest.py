import dash
import dash_core_components as dcc
import dash_html_components as html
import json
import pandas as pd
import dash_table_experiments as dt


print(dcc.__version__) # 0.6.0 or above is required

app = dash.Dash()

 # Since we're adding callbacks to elements that don't exist in the app.layout,
 # Dash will raise an exception to warn us that we might be
 # doing something wrong.
 # In this case, we're adding the elements through a callback, so we can ignore
 # the exception.
app.scripts.config.serve_locally=True
app.config.supress_callback_exceptions = True

app.layout = html.Div([
     dcc.Location(id='url', refresh=False),
     html.Div(id='page-content')
])


index_page = html.Div([
     dcc.Link('Go to Page 1', href='/page-1'),
     html.Br(),
     dcc.Link('Go to Page 2', href='/page-2'),
])

DF_GAPMINDER = pd.read_csv(
     'https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv'
)
DF_GAPMINDER = DF_GAPMINDER[DF_GAPMINDER['year'] == 2007]

page_1_layout = html.Div([
     html.H4('Gapminder DataTable'),
     dt.DataTable(
         rows=DF_GAPMINDER.to_dict('records'),
         filterable=True,
         sortable=True,
         enable_drag_and_drop = False,
         id='datatable-gapminder'
     ),
     dcc.Graph(
         id='graph-gapminder'
     )
 ], className="container")

@app.callback(
     dash.dependencies.Output('graph-gapminder', 'figure'),
     [dash.dependencies.Input('datatable-gapminder', 'rows')])
def update_figure(rows):
     dff = pd.DataFrame(rows)
     fig = plotly.tools.make_subplots(
         rows=3, cols=1,
         subplot_titles=('Life Expectancy', 'GDP Per Capita', 'Population',),
         shared_xaxes=True)
     marker = {'color': '#0074D9'}
     fig.append_trace({
         'x': dff['country'],
         'y': dff['lifeExp'],
         'type': 'bar',
         'marker': marker
     }, 1, 1)
     fig.append_trace({
         'x': dff['country'],
         'y': dff['gdpPercap'],
         'type': 'bar',
         'marker': marker
     }, 2, 1)
     fig.append_trace({
         'x': dff['country'],
         'y': dff['pop'],
         'type': 'bar',
         'marker': marker
     }, 3, 1)
     fig['layout']['showlegend'] = False
     fig['layout']['height'] = 800
     fig['layout']['margin'] = {
         'l': 20,
         'r': 20,
         't': 60,
         'b': 200
     }
     return fig

page_2_layout = html.Div([
     html.H1('Page 2'),
     dcc.RadioItems(
         id='page-2-radios',
         options=[{'label': i, 'value': i} for i in ['Orange', 'Blue', 'Red']],
         value='Orange'
     ),
     html.Div(id='page-2-content'),
     html.Br(),
     dcc.Link('Go to Page 1', href='/page-1'),
     html.Br(),
dcc.Link('Go back to home', href='/')
])

@app.callback(dash.dependencies.Output('page-2-content', 'children'),
               [dash.dependencies.Input('page-2-radios', 'value')])
def page_2_radios(value):
     return 'You have selected "{}"'.format(value)


 # Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
               [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
     if pathname == '/page-1':
         return page_1_layout
     elif pathname == '/page-2':
         return page_2_layout
     else:
         return index_page
     # You could also return a 404 "URL not found" page here

app.css.append_css({
     'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})


if __name__ == '__main__':
     app.run_server(debug=True)
