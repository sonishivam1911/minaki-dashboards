from dash import Dash, html, dash_table, dcc,Input, Output,callback
import numpy as np
import plotly.express as px
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import create_engine



fetch_sku_total_quantity = """
    select cpm.partner_id as id
    ,pm.partner_name_location as name
    ,pm.location as location
    ,pm.partner 
    ,count(distinct coalesce(cpm.sku,cpm.partner_sku)) as sku_unique_count
    ,sum(CAST(cpm.total_quantity as Integer)) as total_quatity
    from challan_product_master cpm 
    left join partner_master pm on cpm.partner_id = pm.partner_id
    --where to_date(cpm.issue_date,'DD/MM/YYYY') >  CURRENT_DATE - INTERVAL '6 months'
    group by 1,2,3,4
    order by total_quatity desc,sku_unique_count desc,id ;
    """



fetch_missing_skus_select_city_walk = """
select distinct cpm.sku as missing_skus
from challan_product_master cpm 
left join product_master pm on pm.sku = coalesce(cpm.sku,cpm.partner_sku)
where cpm.partner_id = 6 and pm.sku is null;
"""


fetch_missing_skus_overall = """
select *
from transaction_master tm 
where partner_sku  not in  (select distinct sku from product_master pm) ;
"""



total_location_sales = """
select 
pm.location as location
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
--where to_date(cpm.issue_date,'DD/MM/YYYY') >  CURRENT_DATE - INTERVAL '6 months'
group by 1
order by total_value desc,total_quatity desc,sku_unique_count desc ;
"""



total_partner_location_sales = """
select tm.partner_id as id
,pm.partner_name_location as name
,pm.location as location
,pm.partner 
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
--where to_date(cpm.issue_date,'DD/MM/YYYY') >  CURRENT_DATE - INTERVAL '6 months'
group by 1,2,3,4
order by total_value desc,total_quatity desc,sku_unique_count desc,id ;
"""



total_partner_location_year_month_sale = """
select tm.partner_id as id
,pm.partner_name_location as name
,pm.location as location
,pm.partner 
,tm.year 
,tm.month 
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
--where to_date(cpm.issue_date,'DD/MM/YYYY') >  CURRENT_DATE - INTERVAL '6 months'
group by 1,2,3,4,5,6
order by year,month,total_value desc,total_quatity desc,sku_unique_count desc,id ;
"""




total_partner_location_product_hierarchy_sale = """
select tm.partner_id as id
,pm.partner_name_location as name
,pm.location as location
,pm2.sub_division 
,pm2.division 
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
left join product_master pm2 on pm2.sku = tm.partner_sku 
where pm2.sub_division is not null or pm2.division is not null  
group by 1,2,3,4,5
order by total_value desc,total_quatity desc,sku_unique_count desc,id ;
"""

monthly_sales_analysis = """
select tm.partner_id as id
,pm.partner_name_location as name
,pm.location as location
,pm.partner 
,tm."year" 
,tm."month" 
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
group by 1,2,3,4,5,6
having tm.partner_id = 6
order by year,month,total_value desc,total_quatity desc,sku_unique_count desc,id ;
"""


product_hierarchy_analysis_scw = """
select tm.partner_id as id
,pm.partner_name_location as name
,pm.location as location
,pm2.sub_division 
,pm2.division 
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
left join product_master pm2 on pm2.sku = tm.sku 
where pm2.sub_division is not null or pm2.division is not null  
group by 1,2,3,4,5
having tm.partner_id = 6
order by total_value desc,total_quatity desc,sku_unique_count desc,id ;
"""

monthly_product_hierarchy_analysis_scw = """
select tm.partner_id as id
,pm.partner_name_location as name
,tm."year" 
,tm."month" 
,pm2.sub_division 
,pm2.division 
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
left join product_master pm2 on pm2.sku = tm.sku 
where pm2.sub_division is not null or pm2.division is not null  
--where to_date(cpm.issue_date,'DD/MM/YYYY') >  CURRENT_DATE - INTERVAL '6 months'
group by 1,2,3,4,5,6
having tm.partner_id = 6
order by tm."year",tm."month";
"""


fetch_transaction_data = "select * from transaction_master"


fetch_sales_data = """
select tm.partner_id as id
,pm.partner as partner_tag
,pm.partner_name_location as name
,tm."year" 
,tm."month" 
,pm2.division
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
left join product_master pm2 on pm2.sku = tm.partner_sku  or pm2.sku = tm.sku
group by 1,2,3,4,5,6
order by tm."year",tm."month";
"""


fetch_sku_sales_data = """
select tm.partner_id as id
,pm.partner_name_location as name
,tm."year" 
,tm."month" 
,tm.partner_sku as partner_sku 
,tm.sku as sku
,pm2.division 
,count(distinct coalesce(tm.sku,tm.partner_sku)) as sku_unique_count
,sum(CAST(tm.quantity_sold  as Integer)) as total_quatity
,sum(CAST(tm.total_value  as Integer)) as total_value
from transaction_master tm  
left join partner_master pm on tm.partner_id = pm.partner_id
left join product_master pm2 on pm2.sku = tm.partner_sku  or pm2.sku = tm.sku
group by 1,2,3,4,5,6,7
order by total_quatity desc,total_value DESC;
"""



def fetch_sqlalchemy_engine(database_username, database_password, database_name='railway',database_ip='monorail.proxy.rlwy.net',database_port = 51989):
    
    database_username = database_username
    database_password = database_password
    database_ip       = database_ip
    database_name     = database_name
    database_port = database_port
    # connection_string = "mysql+mysqlconnector://user:password@host:port/database_name"

    database_connection = create_engine('postgresql://{0}:{1}@{2}:{4}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name,database_port))
    
    return database_connection
    

def execute_query_pandas(query):
    engine = fetch_sqlalchemy_engine("postgres","c*2abGA*eGDg44fbd3-62d255EAb4-B5")
    return pd.read_sql(query,engine)
    

def create_monthly_analysis():
    monthly_analysis = execute_query_pandas(monthly_sales_analysis)
    monthly_analysis.to_csv('monthly_analysis.csv')

    product_hierarchy_analysis_scw = execute_query_pandas(product_hierarchy_analysis_scw)
    product_hierarchy_analysis_scw.to_csv('product_hierarchy_analysis_scw.csv')

    monthly_product_hierarchy_analysis_scw = execute_query_pandas(monthly_product_hierarchy_analysis_scw)
    monthly_product_hierarchy_analysis_scw.to_csv('monthly_product_hierarchy_analysis_scw.csv')

#create_monthly_analysis()




# Incorporate data
df = execute_query_pandas(fetch_sales_data)
df_sku_sales = execute_query_pandas(fetch_sku_sales_data)
df_sku_sales['id'] = df_sku_sales['id'].fillna('Not Found')
df_sku_sales['name'] = df_sku_sales['name'].fillna('Not Found')
df_sku_sales['partner_sku'] = df_sku_sales['partner_sku'].fillna('Not Found')
df_sku_sales['sku'] = df_sku_sales['sku'].fillna('Not Found')
df_sku_sales['division'] = df_sku_sales['division'].fillna('Not Found')
df_sku_sales['sku_unique_count'] = df_sku_sales['sku_unique_count'].fillna(0)
df_sku_sales['total_quatity'] = df_sku_sales['total_quatity'].fillna(0)
df_sku_sales['total_value'] = df_sku_sales['total_value'].fillna(0)

df['partner_tag'] = df['partner_tag'].apply(lambda x : x.strip())

df_partner_year_month = df.groupby(['name','year','month'],as_index=False).agg({
        'sku_unique_count' : sum,
        'total_quatity' : sum,
        'total_value' : sum,
    })


df_partner_tag_year_month = df.groupby(['partner_tag','year','month'],as_index=False).agg({
        'sku_unique_count' : sum,
        'total_quatity' : sum,
        'total_value' : sum,
    })


df_partner_tag_year_month_division = df.groupby(['partner_tag','year','month','division'],as_index=False).agg({
        'sku_unique_count' : sum,
        'total_quatity' : sum,
        'total_value' : sum,
    })

# Initialize the app
# __name__, external_stylesheets=[dbc.themes.ZEPHYR]
app = Dash()
server = app.server

colors = {
    'background': '#b5651d',
    'text': '#7FDBFF'
}



app.layout = dbc.Container([

    dbc.Row(
        dbc.Col(html.H1("Minaki Sales Data",
                        className='text-center text-primary mb-4'),
                width=12)
    ),

    dbc.Row([

        dbc.Col([
            dcc.Dropdown(df["year"].unique(),2023,id='dropdown-year'),
        ],# width={'size':5, 'offset':1, 'order':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),

        dbc.Col([
            dcc.Dropdown(df["month"].unique(),'Dec',id='dropdown-month'),
        ], #width={'size':5, 'offset':0, 'order':2},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),      
        

    ], justify='start'),  # Horizontal:start,center,end,between,around

    dbc.Row([
        
        dbc.Col(html.H2("Monthly Revenue Across All Partners Tags - Aza, Pernia, Taj & SCW",
                        className='text-center text-primary mb-4'),
                width=12),

        dbc.Col([
            dcc.Graph(id='partner-tag-bar',figure={}),
        ], #width={'size':5, 'offset':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),

        dbc.Col(html.H2("Monthly Revenue Across Selected Partner",
                        className='text-center text-primary mb-4'),
                width=12),

        dbc.Col([
            html.H3("Select Partner :",
                   style={"textDecoration": "underline"}),            
            dcc.Dropdown(df["partner_tag"].unique(),'Select City Walk',id='dropdown-partner-tag'),
        ], #width={'size':5, 'offset':0, 'order':2},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),

        dbc.Col([
            html.H3("Monthly Sales Metrics Report:",
                   style={"textDecoration": "underline"}),
            dash_table.DataTable(
                data=df_partner_tag_year_month.to_dict('records'), 
                page_size=20,
                id='table-partner-tag-year-month',
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white'
                },
                style_data={
                    'backgroundColor': 'rgb(50, 50, 50)',
                    'color': 'white'
                },
                )
        ], #width={'size':5, 'offset':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),

        dbc.Col([
            html.H3("Monthly Product Type Revenue Analysis:",
                   style={"textDecoration": "underline"}),
            dcc.Graph(id='partner-division-bar',figure={}),
        ], #width={'size':5, 'offset':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        )
    ], align="center"),  # Vertical: start, center, end

    dbc.Row([


        dbc.Col([
            html.H3("Select Sub-Partner:",
                   style={"textDecoration": "underline"}),              
            dcc.Dropdown(df["name"].unique(),'Select City Walk ',id='dropdown-name'),
        ], #width={'size':5, 'offset':0, 'order':2},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),  

        dbc.Col([
            html.H3("Monthly Sales Analysis - All Sub Partners",
                   style={"textDecoration": "underline"}),
            dash_table.DataTable(
                id='table-partner-year-month',
                columns = [
                    {"name" : i, "id" : i, "selectable" : False}
                    for i in df_partner_year_month.columns
                ],
                data=df_partner_year_month.to_dict('records'), 
                page_size=20,
                editable = False,
                sort_action = 'native',
                sort_mode = 'single',
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white'
                },
                style_data={
                    'backgroundColor': 'rgb(50, 50, 50)',
                    'color': 'white'
                },
                # row_selectable = "multi",
                # selected_rows = [],
                ),
        ], #width={'size':5, 'offset':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),

        dbc.Col([
            html.H3("Monthly Product Sales Analysis",
                   style={"textDecoration": "underline"}),
            dcc.Graph(id='partner-division-month-year-bar',figure={}),
        ], #width={'size':5, 'offset':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),

        dbc.Col([
            html.H3("Monthly Product Sales Table",
                   style={"textDecoration": "underline"}),
            dash_table.DataTable(
                id='table-partner-year-month-division',
                columns = [
                    {"name" : i, "id" : i, "selectable" : False}
                    for i in df.columns
                ],
                data=df.to_dict('records'), 
                page_size=20,
                editable = False,
                sort_action = 'native',
                sort_mode = 'single',
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white'
                },
                style_data={
                    'backgroundColor': 'rgb(50, 50, 50)',
                    'color': 'white'
                },
                # row_selectable = "multi",
                # selected_rows = [],
                ),
        ], #width={'size':5, 'offset':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        )
    ], align="center") ,    

    dbc.Row([
        dbc.Col([
            html.H3("Monthly Product Sales Analysis - SKU Level ( Not filtered by month and year):",
                   style={"textDecoration": "underline"}),
            dash_table.DataTable(
                id='table-partner-sku',
                columns = [
                    {"name" : i, "id" : i, "selectable" : False}
                    for i in df_sku_sales.columns
                ],
                data=df_sku_sales.to_dict('records'), 
                page_size=20,
                editable = False,
                sort_action = 'native',
                sort_mode = 'single',
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white'
                },
                style_data={
                    'backgroundColor': 'rgb(50, 50, 50)',
                    'color': 'white'
                },
                # row_selectable = "multi",
                # selected_rows = [],
                ),
        ], #width={'size':5, 'offset':1},
           xs=12, sm=12, md=12, lg=5, xl=5
        ),

    ], align="center") 

], fluid=True)




@callback(
    Output('table-partner-year-month-division', 'data'),
    Output('table-partner-year-month', 'data'),
    Output('table-partner-tag-year-month', 'data'),
    Output('table-partner-sku', 'data'),
    Output('partner-tag-bar', 'figure'),
    Output('partner-division-bar', 'figure'),
    Output('partner-division-month-year-bar', 'figure'),
    Input('dropdown-year', 'value'),
    Input('dropdown-month', 'value'),
    Input('dropdown-name', 'value'),
    Input('dropdown-partner-tag', 'value'),
)
def update_output(year,month,name,partner_tag):


    df_filtered = df[
        df['year'].eq(year) 
        & df['month'].eq(month) 
        & df['name'].eq(name) 
        ]
    
    df_partner_figure = df_partner_tag_year_month[
        df_partner_tag_year_month['year'].eq(year) 
        & df_partner_tag_year_month['month'].eq(month) 
        ]
    
    df_partner_year_month_new = df_partner_year_month[
        df_partner_year_month['year'].eq(year) 
        & df_partner_year_month['month'].eq(month) 
        & df_partner_year_month['name'].eq(name) 
    ]

    df_partner_tag_year_month_new = df_partner_tag_year_month[
        df_partner_tag_year_month['year'].eq(year) 
        & df_partner_tag_year_month['month'].eq(month) 
        & df_partner_tag_year_month['partner_tag'].eq(partner_tag)
    ]

    df_sku_sales_new = df_sku_sales[
        df_sku_sales['year'].eq(year) 
        & df_sku_sales['month'].eq(month) &
        df_sku_sales['name'].eq(name)
    ]

    df_partner_tag_year_month_division_new = df_partner_tag_year_month_division[
        df_partner_tag_year_month_division['year'].eq(year) 
        & df_partner_tag_year_month_division['month'].eq(month) 
        & df_partner_tag_year_month_division['partner_tag'].eq(partner_tag)
    ]
    df_sku_sales_new[df_sku_sales_new.select_dtypes(np.float64).columns] = df_sku_sales_new.select_dtypes(np.float64).astype(np.float32)

    fig_partner_tag_revenue = px.bar(df_partner_figure, x="partner_tag", y="total_value",text_auto=True)
    fig_partner_tag_revenue_product_type = px.bar(df_partner_tag_year_month_division_new, x="division", y="total_value",text_auto=True)
    fig_partner_tag_revenue_product_type_year_month = px.bar(df_filtered, x="division", y="total_value",text_auto=True)

    return (
        df_filtered.to_dict(orient='records')
        ,df_partner_year_month_new.to_dict(orient='records')
        ,df_partner_tag_year_month_new.to_dict(orient='records')
        ,df_sku_sales_new.to_dict(orient='records')
        ,fig_partner_tag_revenue
        ,fig_partner_tag_revenue_product_type
        ,fig_partner_tag_revenue_product_type_year_month
    )

# Run the app
if __name__ == '__main__':
    app.run(debug=True)