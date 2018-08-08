import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import plotly.graph_objs as go
import plotly.plotly as py
import squarify

import pandas as pd
import numpy as np
import plotly
import pickle
from datetime import datetime
from sqlalchemy import create_engine
import pymysql, pandas as pd
pymysql.install_as_MySQLdb()
import MySQLdb

app = dash.Dash()
app.scripts.config.serve_locally = True

app.layout = html.Div([
    html.Div([dt.DataTable(rows=[{}], id='dt_words_pair')], id='div_words_pair', className="container", style={'width':'40%', 'height':'100%','display':'inline-block'}),
    html.Div([html.Button(id='bt1', n_clicks=0, children="X" ) ], style={'text-align':'left', 'width':'10%', 'height':'100%', 'display': 'inline-block'}),
    html.Div([dt.DataTable(rows=[{}], id='dt_cate_pair')], id='div_cate_pair', className="container", style={'width':'40%', 'height':'100%','display':'inline-block'}),
    #html.Div([html.Button(id='bt2', n_clicks=0, children="Y" ) ], style={'text-align':'left', 'width':'10%', 'height':'100%', 'display': 'inline-block'}),

    html.Div([dt.DataTable(rows=[{}], id='dt_pair_samples')], id='div_pair_samples', className="container", style={'width':'100%', 'height':'100%','display':'inline-block'}),
], id='page', className="container", style={'text-align':'left', 'width':'100%', 'height':'100%','display':'inline-block'})

@app.callback(
    Output('div_words_pair', 'children'),
    [ Input('bt1', 'n_clicks') ],
    [ State('dt_words_pair', 'rows'), State('dt_words_pair', 'selected_row_indices') ]
    )
def loading_pairs(n_clicks, rows, selected_row_indices):
    if n_clicks == 0:
        print('n_clicks == 0')
        return dt.DataTable(
            rows = pc.to_dict('records'),
            columns = ['brand'],
            row_selectable=True,
            filterable=True,
            sortable=True,
            selected_row_indices=[],
            resizable=True,
            max_rows_in_viewport=5,
            #min_width=400,
            id='dt_words_pair'
        )
    else:
        return None

@app.callback(
    Output('div_cate_pair', 'children'),
    [ Input('dt_words_pair', 'selected_row_indices')  ],
    [ State('dt_words_pair', 'rows') ]
    )
def cate_pairs(selected_row_indices, rows):
    if selected_row_indices == None or len(selected_row_indices) == 0:
        return None

    selected_rows = [rows[index] for index in selected_row_indices]
    #print(selected_rows[0]['pair'][0])
    brand_name = selected_rows[0]['brand']
    co_id = original[original.brand == brand_name].co_id.values[0]
    #print(w0, w1)

    engine = create_engine("mysql://eums:eums00!q@133.186.146.142:3306/eums-poi?charset=utf8mb4", encoding = 'utf8' , pool_size=50,pool_recycle=3600,connect_args={'connect_timeout':1000000} )

    query = """
    select CO_NAME,REP_PHONE_NUM,CATE,CATE1,TAG,ADDR,ROAD_ADDR
    from MEUMS_COMPANY
    where ID in {}
    """.format(co_id)
    # print(query)
    #query = 'select CO_NAME,REP_PHONE_NUM,CATE,CATE1,TAG,ADDR,ROAD_ADDR from MEUMS_COMPANY where (STATUS=1 or STATUS=0) and CO_NAME_R regexp "{}.*{}";'.format(w0, w1)
    c = pd.read_sql_query(query, engine)
    #c = c.sample(3000)

    print(len(c), ' db rows')
    freq_cate = pd.DataFrame(c.groupby(['CATE', 'CATE1']).CO_NAME.count()).reset_index().sort_values(by='CO_NAME', ascending=False)
    freq_cate.columns = ['CATE', 'CATE1', 'COUNT']

    return dt.DataTable(
        rows = freq_cate.to_dict('records'),
        #columns = ['pair', 'pair_str', 'cnt'],
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        resizable=True,
        max_rows_in_viewport=5,
        editable=False,
        column_widths=[100,100,300],
        min_width=500,
        id='dt_cate_pair'
    )


@app.callback(
    Output('div_pair_samples', 'children'),
    [ Input('dt_cate_pair', 'selected_row_indices')  ],
    [ State('dt_cate_pair', 'rows'), State('dt_words_pair', 'selected_row_indices'), State('dt_words_pair', 'rows') ]
    )
def querying_pairs(cate_indeces, cate_rows, word_indeces, word_rows):
    if cate_indeces == None or len(cate_indeces) == 0:
        return None
    if word_indeces == None or len(word_indeces) == 0:
        return None

    print(cate_indeces, len(cate_rows), word_indeces, len(word_rows))

    sel_word = [word_rows[i] for i in word_indeces][0]
    sel_cate = [cate_rows[i] for i in cate_indeces][0]

    brand_name = sel_word['brand']
    # print(brand_name)
    co_id = original[original.brand == brand_name].co_id.values[0]

    cate=sel_cate['CATE']
    cate1=sel_cate['CATE1']

    # print(co_id, cate, cate1)

    engine = create_engine("mysql://eums:eums00!q@133.186.146.142:3306/eums-poi?charset=utf8mb4", encoding = 'utf8' , pool_size=50,pool_recycle=3600,connect_args={'connect_timeout':1000000} )
    query = 'select CO_NAME,REP_PHONE_NUM,CATE,CATE1,TAG,ADDR,ROAD_ADDR from MEUMS_COMPANY where ID in {} and CATE="{}" and CATE1="{}";'.format(co_id, cate, cate1)
    c = pd.read_sql_query(query, engine)

    return dt.DataTable(
        rows = c.to_dict('records'),
        #columns = ['pair', 'pair_str', 'cnt'],
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        resizable=True,
        max_rows_in_viewport=20,
        editable=False,
        column_widths=[300,100,80,100,400,350,350],
        min_width=1800,
        id='dt_pair_samples'
    )



app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    original = pd.read_pickle('brand_candidate.pkl')
    pc = original[['brand']]
    app.run_server(debug=True)
