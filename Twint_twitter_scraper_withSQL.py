import pandas as pd
import regex as re
from nltk.corpus import stopwords
from datetime import date
from pathlib import Path 
import dash
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import twint
import nest_asyncio
import sqlite3 as sql
nest_asyncio.apply()

c = twint.Config()
stop = stopwords.words('english')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

Twitter_scraper = Dash(__name__, external_stylesheets=external_stylesheets)

Twitter_scraper.layout = html.Div([
    html.H6('Twint Twitter Scraper'),

    html.Div([
        html.Div([
            html.P("Search: ", style = {'display': 'inline-block'}),
            dcc.Input(id='search', type='text', 
                      placeholder = '',
                      style = {"marginLeft": "26px"})
            ]),
        html.Div([
            "Username: ",
            dcc.Input(id='username', type='text')
            ]),
        dcc.DatePickerRange(children = 'Data range', id='date_range',
                            max_date_allowed=date(2022, 4, 15),
                            initial_visible_month=date(2022, 4, 15),
                            ),
        html.Br(),
        html.Button(id='submit-button-state', 
                    n_clicks=0, children='Submit'),
        dcc.ConfirmDialog(id='confirm_nodata',
                          message='No data whithin selection',
                          ),
        html.Div([
            html.P("Remove from tweets:", style = {"marginTop": "10px",
                                                   "marginBottom": "0px"}),
            dcc.Checklist(['URLs', 'special signs', 'hashtags and mentions', 
                           'stopwords'], id='remove')
            ]),
        html.Div([
            "Change letter case ",
            dcc.Dropdown(['No', 'Upper case', 'Lower case'], 'No', 
                         id='case_option',
                         style={"width": "140px", 'display': 'inline-block', 
                                'verticalAlign': 'middle'})
            ]),
        html.Div(["Store options: ",
                  dcc.RadioItems(['csv', 'json', 'xlsx'], 'csv', 
                                 id='store_options')
                  ]),
        html.Div([
            html.P("File name: ", style = {'display': 'inline-block'}),
            dcc.Input(id='file_name', type='text', style = {"marginLeft": "7px"})
            ]),
        html.Button('Download', id='download', n_clicks=0),
        dcc.ConfirmDialog(id='confirm_download',
                          message='Download sucessful',
                          )],
        style={'display': 'inline-block', 'verticalAlign': 'top'}),    
    html.Div(
        dcc.Loading(
                    id="loading",
                    children=[html.Table(id='table'), dcc.Store(id='scrape_data')],
                    type="circle"
                ),
        style={'display': 'inline-block', 'margin-left': '20px'}
            ),
        dcc.Store(id='filtered_data')
    
])
 
                                      
                                      
@Twitter_scraper.callback(Output('scrape_data', 'data'),
                          Output('confirm_nodata', 'displayed'),
                          Input('submit-button-state', 'n_clicks'),
                          State('search', 'value'),
                          State('username', 'value'),
                          State('date_range', 'start_date'),
                          State('date_range', 'end_date'),
                          )
def update_output(n_clicks, search, username, start_date, end_date):
    if n_clicks:
        if search == '':
            search = None
        if username == '':
            username = None
        c.Search = search
        c.Username = username
        c.Pandas = True
        c.Limit = 100
        c.Since = start_date
        c.Until = end_date
        c.Hide_output = True
        twint.run.Search(c)
        Tweets_df = twint.storage.panda.Tweets_df
        if not Tweets_df.empty:
            Tweets_df = Tweets_df[['id', 'username', 'tweet', 'date', 'language']]
            Tweets_df = Tweets_df[Tweets_df['language'] == 'en']
            Tweets_df = Tweets_df.drop(columns = 'language')
            return Tweets_df.to_json(date_format='iso', orient='split'), False
        else:
            return dash.no_update, True
    return dash.no_update, dash.no_update

@Twitter_scraper.callback(Output('table', 'children'),
                          Output('filtered_data', 'data'),
                          Input('scrape_data', 'data'),
                          Input('remove', 'value'),
                          Input('case_option', 'value'))
def update_table(jsonified_cleaned_data, remove, case_option):
    try:
        dff = pd.read_json(jsonified_cleaned_data, orient='split')
    except:
        raise PreventUpdate()
    dff['date'] = dff['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(remove, list):
        if 'stopwords' in remove:
            dff['tweet'] = dff['tweet'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))
        if 'hashtags and mentions' in remove:
            for i in range(dff.shape[0]):
                dff.iloc[i, 2] = ' '.join(re.sub("(@[A-Za-z0-9]+)"," ", dff.iloc[i, 2]).split())       
        if 'special signs' in remove:
            for i in range(dff.shape[0]):
                dff.iloc[i, 2] = ' '.join(re.sub("([^0-9A-Za-z \t])"," ", dff.iloc[i, 2]).split())
        if 'URLs' in remove:
            for i in range(dff.shape[0]):
                dff.iloc[i, 2] = ' '.join(re.sub("(\w+:\/\/\S+)"," ", dff.iloc[i, 2]).split())
    if case_option == 'Upper case':
        dff['tweet'] = dff['tweet'].str.upper()
    elif case_option == 'Lower case':
        dff['tweet'] = dff['tweet'].str.lower()
            
    table = dash_table.DataTable(dff.to_dict('records'),
                                 [{"name": i, "id": i} for i in dff.columns],
                                 fixed_rows={'headers': True},
                                 style_data={'whiteSpace': 'normal',
                                             'height': 'auto',
                                             'lineHeight': '15px',
                                             'textAlign': 'left',
                                             'overflowY': 'auto'},
                                 style_cell_conditional=[{'if': {'column_id': 'tweet'},
                                                          'width': '600px'}],
                                 fill_width = False)
    return table, dff.to_json(date_format='iso', orient='split')

@Twitter_scraper.callback(Output('confirm_download', 'displayed'),
                          Input('download', 'n_clicks'),
                          State('filtered_data', 'data'),
                          State('store_options', 'value'),
                          State('file_name', 'value'))
def update_output(n_clicks, jsonified_cleaned_data, store_options, file_name):
    ctx = dash.callback_context

    if ctx.triggered:
        path = 'data/' + file_name + '.' + store_options
        path1 = file_name
        dff = pd.read_json(jsonified_cleaned_data, orient='split')
        filepath = Path(path)
        filepath1 = Path(path1)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        if store_options == 'csv':
            con = sql.connect('example.db')
            dff.to_sql(filepath1, con, if_exists='replace')
            dff.to_csv(filepath, index = False)
        elif store_options == 'json':
            dff.to_json(filepath, orient='split')
        else:
            dff.to_excel(filepath, index = False)
        return True
    return False

if __name__ == '__main__':
    Twitter_scraper.run_server(debug=False)
   