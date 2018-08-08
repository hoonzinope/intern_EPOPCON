# Perform imports here:
import numpy as np
import pandas as pd
import plotly.offline as pyo
import plotly.graph_objs as go
import matplotlib.pyplot as plt
import dash
import dash_core_components as dcc
import dash_html_components as html
import datetime
from sqlalchemy import create_engine
from dash.dependencies import Input, Output, State
import math
from math import radians, cos, sin, asin, sqrt
import dash_table_experiments as dt
from fastparquet import ParquetFile

category = pd.read_pickle('category.pkl')
site = pd.read_pickle('site.pkl')

app = dash.Dash()
app.scripts.config.serve_locally = True

big_OPTION = []
mid_OPTION = []
small_OPTION = []
big_cate_list = category[category.DEPTH == 1][['CATE_NO', 'CATE_NAME']].reset_index()
for idx in range(0,len(big_cate_list)):
    big_OPTION.append({'label':big_cate_list['CATE_NAME'][idx], 'value': big_cate_list['CATE_NO'][idx]})


app.layout = html.Div([
        html.Div([dcc.Dropdown(id = 'big_name',options = big_OPTION,value = 'ID'),
                dcc.Dropdown(id = 'mid_name',options = mid_OPTION,value = 'ID'),
                dcc.Dropdown(id = 'small_name',options = small_OPTION,value = 'ID')], style = {'width': 400,'height':70,"text-align":"center"}),
        html.Br(),
        html.Div([html.Button(id = 'submit-button', n_clicks= 0, children = 'Submit', style = {'fontSize':20, 'height':100})],),
        #html.Div([dt.DataTable(rows=[{}], id='dt_data_pair')], id='div_data_pair', className="container", style={'width': "100%",'display':'inline-block'}),
        html.Div([html.Img(id = 'image',),], id = 'div_image', style={'width': "100%",'display':'inline-block'}),
])
@app.callback(
    Output('mid_name', 'options'),
    [Input('big_name', 'value')],
)
def get_mid_name(cate_no):
    if cate_no == None:
        return None

    mid_OPTION = []
    global category
    mid_OPTION = []
    midcate = category[category.UPPER_CATE_NO == cate_no]
    mid_cate_list = midcate[midcate.DEPTH == 2][['CATE_NO', 'CATE_NAME']].reset_index()
    for idx in range(0,len(mid_cate_list)):
        mid_OPTION.append({'label':mid_cate_list['CATE_NAME'][idx], 'value': mid_cate_list['CATE_NO'][idx]})

    return mid_OPTION

@app.callback(
    Output('small_name', 'options'),
    [Input('mid_name', 'value')],
)
def get_small_name(cate_no):
    if cate_no == None:
        return None

    small_OPTION = []
    global category
    smallcate = category[category.UPPER_CATE_NO == cate_no]
    small_cate_list = smallcate[smallcate.DEPTH == 3][['CATE_NO', 'CATE_NAME']].reset_index()
    for idx in range(0,len(small_cate_list)):
        small_OPTION.append({'label':small_cate_list['CATE_NAME'][idx], 'value': small_cate_list['CATE_NO'][idx]})

    return small_OPTION

@app.callback(
    Output('div_image', 'children'),
    [Input('submit-button', 'n_clicks')],
    [State('small_name', 'value')]
)
def returnID_list(n_click, cate_no):
    wspidermr_engine = create_engine("mysql+pymysql://wspidermr:wspidermr00!q@133.186.134.155:3306/lf-bigdata-real-5?charset=utf8mb4",
                       encoding = 'utf8',
                       pool_size=20,
                       pool_recycle=3600,
                       connect_args={'connect_timeout':1000000} )
    wspider_engine = create_engine("mysql+pymysql://wspider:wspider00!q@133.186.143.65:3306/wspider?charset=utf8mb4",
                       encoding = 'utf8',
                       pool_size=20,
                       pool_recycle=3600,
                       connect_args={'connect_timeout':1000000} )



    query = """select * from MLF_GOODS_CATE where CATE_NO = {} limit 10000""".format(cate_no)
    good_no = pd.read_sql_query(query, wspidermr_engine)
    goods_no = good_no.GOODS_NO.unique()


    query = """select * from MLF_GOODS where GOODS_NO in {} limit 10000 """.format(tuple(goods_no))
    goods = pd.read_sql_query(query, wspidermr_engine)
    goods = pd.merge(goods,site, on = 'SITE_NO')
    goods = goods[['SITE_NAME', 'ITEM_NUM']]
    sitenames = goods.SITE_NAME.unique()

    ID_list = []
    for name in sitenames:
        ID = tuple(goods[goods.SITE_NAME == name].ITEM_NUM.unique())
        query = """
        select ID
        from MWS_COLT_ITEM
        where ITEM_NUM in {} and SITE_NAME = '{}'
        limit 1000
        """.format(ID, name)
        temp = pd.read_sql_query(query, wspider_engine)
        print(len(temp),name)
        if len(temp) == 0:
            continue
        else:
            ID_list.append(temp)

    if len(ID_list) == 1:
        id_list = ID_list
    else:
        id_list = pd.concat(ID_list)

    print(len(id_list))

    query = """
    select GOODS_IMAGE
    from MWS_COLT_IMAGE
    where ITEM_ID in {}
    limit 2000
    """.format(tuple(id_list['ID'].unique()))
    image_list = pd.read_sql_query(query, wspider_engine)
    print(len(image_list))
    return [html.Img(id = 'image',src = image,height = 150, width = 150)for image in list(image_list['GOODS_IMAGE'])]
    # return dt.DataTable(
    # rows = image_list.to_dict('records'),
    # #columns = ['pair', 'pair_str', 'cnt'],
    # row_selectable=True,
    # filterable=True,
    # sortable=True,
    # selected_row_indices=[],
    # resizable=True,
    # max_rows_in_viewport=5,
    # editable=False,
    # min_width=2000,
    # id='dt_data_pair'
    # )

app.css.append_css({
     'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
 })

if __name__ == '__main__':
    app.run_server(debug = True)
