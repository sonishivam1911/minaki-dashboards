from dash import Dash, html, dash_table, dcc,Input, Output,callback
import plotly.express as px
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
,pm.partner_name_location as location_name
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
--where pm2.sub_division is not null or pm2.division is not null  
--where to_date(cpm.issue_date,'DD/MM/YYYY') >  CURRENT_DATE - INTERVAL '6 months'
group by 1,2,3,4,5,6,7
--having tm.partner_id = 6
order by tm."year",tm."month";
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


# Initialize the app
app = Dash(__name__)
server = app.server


# App layout
app.layout = html.Div([
    html.Div(children=[
        html.Label('Minaki Transaction Data'),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Br(),
    ]),

    html.Div(children=[
        dcc.Dropdown(df["year"].unique(),2023,id='dropdown-year'),
        dcc.Dropdown(df["month"].unique(),'Dec',id='dropdown-month'),
        dcc.Dropdown(df["name"].unique(),'Select City Walk ',id='dropdown-name'),
        html.Br()
    ]),
    
    html.Div(children=[
        html.Label('Partner Year Month Division Report'),
        html.Br(),
        dash_table.DataTable(data=df.to_dict('records'), page_size=20,id='table-partner-year-month-division'),
        html.Br(),
    ]),

    html.Div(children=[
        html.Label('Partner Year Month Report'),
        html.Br(),
        dash_table.DataTable(data=df_partner_year_month.to_dict('records'), page_size=20,id='table-partner-year-month'),
        html.Br(),
        html.Br(),
        html.Br(),
        html.Br(),
    ]),
    

    html.Div(children=[
        html.Br(),
        dcc.Dropdown(df["partner_tag"].unique(),'Select City Walk',id='dropdown-partner-tag'),
        html.Br(),
        html.Label('Partner-Tag Year Month Report'),
        html.Br(),
        dash_table.DataTable(data=df_partner_tag_year_month.to_dict('records'), page_size=20,id='table-partner-tag-year-month')
    ]),

    html.Div(children=[
        html.Br(),
        dcc.Graph(id='partner-tag-bar',figure={})
    ])

])




@callback(
    Output('table-partner-year-month-division', 'data'),
    Output('table-partner-year-month', 'data'),
    Output('table-partner-tag-year-month', 'data'),
    Output('partner-tag-bar', 'figure'),
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

    fig_partner_tag_revenue = px.bar(df_partner_figure, x="partner_tag", y="total_value",text_auto=True)

    return (
        df_filtered.to_dict(orient='records')
        ,df_partner_year_month_new.to_dict(orient='records')
        ,df_partner_tag_year_month_new.to_dict(orient='records')
        ,fig_partner_tag_revenue
    )

# Run the app
if __name__ == '__main__':
    app.run(debug=True)