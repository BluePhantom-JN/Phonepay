import streamlit as st
import json
import os
import pandas as pd
import mysql.connector
from streamlit_option_menu import option_menu
from PIL import Image
import streamlit as st
import plotly.express as px
import requests

# ! git clone https://github.com/PhonePe/pulse.git

# MySQL connection
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='phonepay'
)
cursor = connection.cursor()

# (((((((((((((((((((((((((((((((((((((()))((((((((((((((((((())))))))))))))))))))))))))))))))))))))))))))))))))))))

# Function for data extraction
def extract_transaction_data(path):
    agg_state_list = os.listdir(path)

    for state in agg_state_list:
        p_state = os.path.join(path, state)
        agg_year_list = os.listdir(p_state)

        for year in agg_year_list:
            p_year = os.path.join(p_state, year)
            agg_quarter_list = os.listdir(p_year)

            for quarter in agg_quarter_list:
                p_quarter = os.path.join(p_year, quarter)
                with open(p_quarter, 'r') as data:
                    D = json.load(data)
                yield state, year, int(quarter.strip(".json")), D

# Fuction for Convert DataFrame to tuple list for SQL insertion
def get_list_values(values):
    return [tuple(x) for x in values.to_numpy()]

# ((((((((((((((((((((((((((((((((((((((((((((((((((((((((()))))))))))))))))))))))))))))))))))))))))))))))))))))))))
# EXTRACT DATA
# 1*1 aggr_transaction 
## Data Path 
path = r"G:\python\pulse\data\aggregated\transaction\country\india\state"
## Define schema
clm = {'state': [], 'Year': [], 'Quater': [], 'Transacion_type': [], 'Transacion_count': [], 'Transacion_amount': []}
query = """
    CREATE TABLE IF NOT EXISTS aggr_trans (
        state VARCHAR(100),
        Year INT,
        Quarter INT,
        Transaction_type VARCHAR(100),
        Transaction_count BIGINT,
        Transaction_amount BIGINT,
        CONSTRAINT pk_aggr_trans PRIMARY KEY(state, Year, Quarter, Transaction_type)
    )
"""
cursor.execute(query)
## Parse and insert data
for state, year, quarter, D in extract_transaction_data(path):
    user_data = D.get("data", {}).get("transactionData")
    if user_data:
        for z in user_data:
            clm['state'].append(state)
            clm['Year'].append(year)
            clm['Quater'].append(quarter)
            clm['Transacion_type'].append(z['name'])
            clm['Transacion_count'].append(z['paymentInstruments'][0]['count'])
            clm['Transacion_amount'].append(int(z['paymentInstruments'][0]['amount']))
aggr_trans = pd.DataFrame(clm)
query = """
    INSERT IGNORE INTO aggr_trans 
    (state, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount) 
    VALUES (%s, %s, %s, %s, %s, %s)
"""
values = get_list_values(aggr_trans)
cursor.executemany(query, values)
connection.commit()


# 1*2 aggregated/user/
## Check if 'data' and 'usersByDevice' exist and are not None

                  #   if D.get('data') and D['data'].get('usersByDevice') is not None:

      # this code help in getting non null data avoid error
path =r"G:\python\pulse\data\aggregated\user\country\india\state"
## Define schema
clm={'state':[],'year':[],'quater':[],'brand':[],'count':[],'percentage':[]}
clm1={'state':[],'year':[],'quater':[],'registeredUsers':[],"appOpens":[]}
query="""CREATE TABLE if not exists aggr_user 
(
    state      VARCHAR(100),
    year       INT,
    quater     INT,
    brand      VARCHAR(100),
    count      INT,
    percentage FLOAT,
    CONSTRAINT pk_aggr_user PRIMARY KEY (state, year, quater, brand)
) """
cursor.execute(query)
## parse and insert data
for state,year,quater,D in extract_transaction_data(path):
      user_data=D['data']['usersByDevice']
      if user_data:
        for z in user_data:
          clm['state'].append(state)
          clm['year'].append(year)
          clm['quater'].append(quater)
          clm['brand'].append(z['brand'])
          clm['count'].append(z['count'])
          clm['percentage'].append(f"{z['percentage'] * 100:.2f}%")

      user_data=D['data']['aggregated']
      clm1['state'].append(state)
      clm1['year'].append(year)
      clm1['quater'].append(quater)
      clm1['registeredUsers'].append(user_data['registeredUsers'])
      clm1['appOpens'].append(user_data['appOpens'])
aggre_user_usersByDevice=pd.DataFrame(clm)
aggre_user_aggregated=pd.DataFrame(clm1)
query ='insert ignore into aggr_user (state,year,quater,brand,count,percentage) values(%s,%s,%s,%s,%s,%s)'
cursor.executemany(query,get_list_values(aggre_user_usersByDevice))
connection.commit()

# 1*3 aggregated/insurance
path=r"G:\python\pulse\data\aggregated\insurance\country\india\state"
clm = {'state':[],'year':[],'quater':[],'type':[],'count':[],'amount':[]}
query=""" create table if not exists aggr_insurance (
  state varchar(255),
  year int,
  quater int,
  type varchar(255),
  count int,
  amount int,
  constraint pk primary key (state,year,quater,type)
)
"""
cursor.execute(query)
for state,year,quater,D in extract_transaction_data(path):
        user_data=D['data']['transactionData']
        #user_data=D.get('data',{}).get('transactionData')
        if user_data:
          for z in user_data:
            clm['state'].append(state)
            clm['year'].append(year)
            clm['quater'].append(quater)
            clm['type'].append(z['paymentInstruments'][0]['type'])
            clm['count'].append(z['paymentInstruments'][0]['count'])
            clm['amount'].append(z['paymentInstruments'][0]['amount'])
aggre_insurance=pd.DataFrame(clm)
query="insert ignore into aggr_insurance(state,year,quater,type,count,amount) values(%s,%s,%s,%s,%s,%s)"
cursor.executemany(query,get_list_values(aggre_insurance))
connection.commit()

# 2*1 map/insurance

path = r"G:\python\pulse\data\map\insurance\country\india\state"
clm = {'state':[],'year':[],'quater':[],'lat':[],'lng':[],'metric':[],'label':[]}
query=""" create table if not exists map_insurance (
  state varchar(255),
  year int,
  quater int,
  lat float,
  lng float,
  metric int,
  label varchar(255),
  constraint pk primary key (state,year,quater)
  )
"""
cursor.execute(query)
for state,year,quater,D in extract_transaction_data(path):
        user_data=D['data']['data']['data']
        if user_data:
            for z in user_data:
                clm['state'].append(state)
                clm['year'].append(year)
                clm['quater'].append(quater)

                clm['lat'].append(float(z[0]))
                clm['lng'].append(float(z[1]))
                clm['metric'].append(z[2])
                clm['label'].append(z[3])
map_insurance=pd.DataFrame(clm)
query=""" create table if not exists map_insurance (
  state varchar(255),
  year int,
  quater int,
  lat float,
  lng float,
  metric int,
  label varchar(255)
  )
"""
#cursor.execute(query)

# 2*2 map/transaction/
path=r"G:\python\pulse\data\map\transaction\hover\country\india\state"
clm = {'state':[],'year':[],'quater':[],'dist_name':[],'type':[],'count':[],'ammount':[]}
query=""" create table if not exists map_transaction (
  state varchar(255),
  year int,
  quater int,
  dist_name varchar(255),
  type varchar(255),
  count int,
  ammount int,
  constraint pk primary key (state,year,quater,dist_name,type)
  )
"""
cursor.execute(query)
for state,year,quater,D in extract_transaction_data(path):
        user_data=D['data']['hoverDataList']
        # data is in list[list[data]] so use for loop to extract data
        for x in user_data:
          for y in x['metric']:
            clm['state'].append(state)
            clm['year'].append(year)
            clm['quater'].append(quater)

            clm['dist_name'].append(x['name'])
            clm['type'].append(y['type'])
            clm['count'].append(y['count'])
            clm['ammount'].append(int(y['amount']))

map_transaction=pd.DataFrame(clm)
query="insert ignore into map_transaction(state,year,quater,dist_name,type,count,ammount) values(%s,%s,%s,%s,%s,%s,%s)"
cursor.executemany(query,get_list_values(map_transaction))
connection.commit()

# 2*3 map/user

path=r"G:\python\pulse\data\map\user\hover\country\india\state"
clm = {'state':[],'year':[],'quater':[],'dist_name':[],'registeredUsers':[],'appOpen':[]}
query=""" create table if not exists map_user (
  state varchar(255),
  year int,
  quater int,
  dist_name varchar(255),
  registeredUsers int,
  appOpen int,
  constraint pk primary key (state,year,quater,dist_name)
  )
"""
cursor.execute(query)
for state,year,quater,D in extract_transaction_data(path):
        user_data=D['data']['hoverData']
        # data is in list[list[data]] so use for loop to extract data
        for dist_name,metrics in user_data.items():
          registereduser=metrics['registeredUsers']
          appOpen=metrics['appOpens']
          clm['state'].append(state)
          clm['year'].append(year)
          clm['quater'].append(quater)
          clm['dist_name'].append(dist_name)
          clm['registeredUsers'].append(registereduser)
          clm['appOpen'].append(appOpen)
map_user=pd.DataFrame(clm)
query="insert ignore into map_user(state,year,quater,dist_name,registeredUsers,appOpen) values(%s,%s,%s,%s,%s,%s)"
cursor.executemany(query,get_list_values(map_user))
connection.commit()

# 3*1 top/insurance
path = r"G:\python\pulse\data\top\insurance\country\india\state"
clm = {'state':[],'year':[],'quater':[],'dist_name':[],'count':[],'ammount':[] }
query=""" create table if not exists top_insurance (
  state varchar(255),
  year int,
  quater int,
  dist_name varchar(255),
  count int,
  ammount int,
  constraint pk primary key (state,year,quater,dist_name)
  )
"""
cursor.execute(query)
for state,year,quater,D in extract_transaction_data(path):
        user_data=D['data']['districts']
        for dist in user_data:
          clm['state'].append(state)
          clm['year'].append(year)
          clm['quater'].append(quater)
          clm['dist_name'].append(dist['entityName'])
          clm['count'].append(dist['metric']['count'])
          clm['ammount'].append(dist['metric']['amount'])
top_insurance=pd.DataFrame(clm)
query="insert ignore into top_insurance(state,year,quater,dist_name,count,ammount) values(%s,%s,%s,%s,%s,%s)"
cursor.executemany(query,get_list_values(top_insurance))
connection.commit()

# 3*2  top/transaction
path =r"G:\python\pulse\data\top\transaction\country\india\state"
clm = {'state':[],'year':[],'quater':[],'dist_name':[],'count':[],'trans_amount':[]}
query=""" create table if not exists top_transaction (
  state varchar(255),
  year int,
  quater int,
  dist_name varchar(255),
  count int,
  trans_amount int,
  constraint pk primary key (state,year,quater,dist_name)
  )
"""
cursor.execute(query)
for state,year,quater,D in extract_transaction_data(path):
        user_data=D['data']['districts']
        for x in user_data:
          clm['state'].append(state)
          clm['year'].append(year)
          clm['quater'].append(quater)
          clm['count'].append(x['metric']['count'])
          clm['trans_amount'].append(int(x['metric']['amount']))
          clm['dist_name'].append(x['entityName'])

top_transaction=pd.DataFrame(clm)
query="insert ignore into top_transaction(state,year,quater,dist_name,count,trans_amount) values(%s,%s,%s,%s,%s,%s)"
cursor.executemany(query,get_list_values(top_transaction))
connection.commit()

# 3*3 top/user
path=r"G:\python\pulse\data\top\user\country\india\state"
clm = {'state':[],'year':[],'quater':[],'pincode':[],'registeredUsers':[]}

query=""" create table if not exists top_user (
  state varchar(255),
  year int,
  quater int,
  pincode int,
  registeredUsers int,
  constraint pk primary key (state,year,quater,pincode)
  )
"""
cursor.execute(query)
for state,year,quater,D in extract_transaction_data(path):
        user_data=D['data']['pincodes']
        for x in user_data:
          clm['state'].append(state)
          clm['year'].append(year)
          clm['quater'].append(quater)
          clm['pincode'].append(x['name'])
          clm['registeredUsers'].append(x['registeredUsers'])
top_user=pd.DataFrame(clm)
query="insert ignore into top_user(state,year,quater,pincode,registeredUsers) values(%s,%s,%s,%s,%s)"
cursor.executemany(query,get_list_values(top_user))
connection.commit()

# (((((((((((((((((((((((((((((((((((((((((((((( ((((((((( ))) ))))))))))))))))))))))))))))))))))))))))))))))))))))

## Load tables into dataframes
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
for (table_name,) in tables:
    globals()[table_name] = pd.read_sql(f"SELECT * FROM `{table_name}`", connection)
    print("loaded")

# state name changing as per json
def state_name_order(df):
    state_name_mapping = {
    'andaman-&-nicobar-islands': 'Andaman & Nicobar',
    'andhra-pradesh': 'Andhra Pradesh',
    'arunachal-pradesh': 'Arunachal Pradesh',
    'assam': 'Assam',
    'bihar': 'Bihar',
    'chandigarh': 'Chandigarh',
    'chhattisgarh': 'Chhattisgarh',
    'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'delhi': 'Delhi',
    'goa': 'Goa',
    'gujarat': 'Gujarat',
    'haryana': 'Haryana',
    'himachal-pradesh': 'Himachal Pradesh',
    'jammu-&-kashmir': 'Jammu & Kashmir',
    'jharkhand': 'Jharkhand',
    'karnataka': 'Karnataka',
    'kerala': 'Kerala',
    'ladakh': 'Ladakh',
    'lakshadweep': 'Lakshadweep',
    'madhya-pradesh': 'Madhya Pradesh',
    'maharashtra': 'Maharashtra',
    'manipur': 'Manipur',
    'meghalaya': 'Meghalaya',
    'mizoram': 'Mizoram',
    'nagaland': 'Nagaland',
    'odisha': 'Odisha',
    'puducherry': 'Puducherry',
    'punjab': 'Punjab',
    'rajasthan': 'Rajasthan',
    'sikkim': 'Sikkim',
    'tamil-nadu': 'Tamil Nadu',
    'telangana': 'Telangana',
    'tripura': 'Tripura',
    'uttar-pradesh': 'Uttar Pradesh',
    'uttarakhand': 'Uttarakhand',
    'west-bengal': 'West Bengal'
    }
    df['state'] = df['state'].replace(state_name_mapping)
    return df

# Data Insight 

# (((((((((((((((((((((((((((((((((((((((((((((((((((((((((      )))))))))))))))))))))))))))))))))))))))))))))))))))))))))

# ploting data 

# plot_aggregated_transaction_pie_   
def plot_aggregated_transaction_pie_charts(aggr_trans):
    # Calculate total transaction amount and count
    total_amount = aggr_trans['Transaction_amount'].sum()
    total_count = aggr_trans['Transaction_count'].sum()

    # Group data by transaction type
    chart1 = aggr_trans.groupby('Transaction_type')['Transaction_amount'].sum().reset_index()
    chart2 = aggr_trans.groupby('Transaction_type')['Transaction_count'].sum().reset_index()

    # Layout: two columns
    col1, col2 = st.columns(2)

    # Plot Transaction Amount Pie Chart
    with col1:
        fig_amount = px.pie(
            chart1,
            names='Transaction_type',
            values='Transaction_amount',
            title=f'Transaction Type by Transaction Amount ({int(total_amount / 1e12)}T)',
            hole=0.3
        )
        st.plotly_chart(fig_amount)

    # Plot Transaction Count Pie Chart
    with col2:
        fig_count = px.pie(
            chart2,
            names='Transaction_type',
            values='Transaction_count',
            title=f'Transaction Type by Transaction Count ({int(total_count / 1e9)}M)',
            hole=0.3
        )
        st.plotly_chart(fig_count)

# plot_top_transaction_analysis(aggr_trans):
def plot_top_transaction_analysis(aggr_trans):
    st.subheader("Top States by Transaction Amount and Count")

    # Compute top states by transaction amount
    state_total = aggr_trans.groupby('state')['Transaction_amount'].sum().sort_values(ascending=False)
    sorted_state = state_total.index.tolist()

    # Prepare data for stacked bar: Transaction Amount
    df_amount = aggr_trans.groupby(['state', 'Transaction_type'])['Transaction_amount'].sum().reset_index()
    df_amount = df_amount[df_amount['state'].isin(sorted_state)]
    df_amount['state'] = pd.Categorical(df_amount['state'], categories=sorted_state, ordered=True)

    fig_amount = px.bar(
        df_amount,
        x='state',
        y='Transaction_amount',
        color='Transaction_type',
        title="Top States by Transaction Amount (Stacked)",
        height=600
    )
    fig_amount.update_layout(barmode='stack', xaxis_title='state', yaxis_title='Amount')
    st.plotly_chart(fig_amount)

    # Prepare data for stacked bar: Transaction Count
    df_count = aggr_trans.groupby(['state', 'Transaction_type'])['Transaction_count'].sum().reset_index()
    df_count = df_count[df_count['state'].isin(sorted_state)]
    df_count['state'] = pd.Categorical(df_count['state'], categories=sorted_state, ordered=True)

    fig_count = px.bar(
        df_count,
        x='state',
        y='Transaction_count',
        color='Transaction_type',
        title="Top States by Transaction Count (Stacked)",
        height=600
    )
    fig_count.update_layout(barmode='stack', xaxis_title='state', yaxis_title='Count')
    st.plotly_chart(fig_count)

# plot_transaction_clustered_bars(aggr_trans):
def plot_transaction_clustered_bars(aggr_trans):
    st.subheader("Transaction Amount by state (Grouped by Year and Quarter)")

    # Get top 20 states by total transaction amount
    sorted_state = aggr_trans.groupby('state')['Transaction_amount'].sum()\
        .sort_values(ascending=False).head(20).index.tolist()

    # ========== Chart 1: Year-wise ========== #
    chart1 = aggr_trans.groupby(['state', 'Year'])['Transaction_amount'].sum().reset_index()
    chart1 = chart1[chart1['state'].isin(sorted_state)]
    chart1['state'] = pd.Categorical(chart1['state'], categories=sorted_state, ordered=True)

    fig_year = px.bar(
        chart1,
        x='state',
        y='Transaction_amount',
        color='Year',
        title="Transaction Amount by state and Year",
        barmode='group',
        color_discrete_sequence=px.colors.sequential,
        height=600
    )
    fig_year.update_layout(xaxis_title="state", yaxis_title="Transaction Amount", xaxis_tickangle=45)
    st.plotly_chart(fig_year)

    # ========== Chart 2: Quarter-wise ========== #
    chart2 = aggr_trans.groupby(['state', 'Quarter'])['Transaction_amount'].sum().reset_index()
    chart2 = chart2[chart2['state'].isin(sorted_state)]
    chart2['state'] = pd.Categorical(chart2['state'], categories=sorted_state, ordered=True)

    fig_quarter = px.bar(
        chart2,
        x='state',
        y='Transaction_amount',
        color='Quarter',
        title="Transaction Amount by state and Quarter",
        barmode='group',
        color_discrete_sequence=px.colors.sequential,
        height=600
    )
    fig_quarter.update_layout(xaxis_title="state", yaxis_title="Transaction Amount", xaxis_tickangle=45)
    st.plotly_chart(fig_quarter)

# plot_top_phone_brands(aggr_user)
def plot_top_phone_brands(aggr_user):
    st.subheader("Top Phone Brands by User Count")

    # Group and sort top brands
    df = aggr_user.groupby('brand')['count'].sum().sort_values(ascending=False).head(10).reset_index()

    # Create pie chart
    fig = px.pie(
        df,
        names='brand',
        values='count',
        title='Top 10 Brands of Phone Users',
        hole=0.3,  # For a donut chart, optional
        color_discrete_sequence=px.colors.sequential.Plasma_r
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')

    # Show in Streamlit
    st.plotly_chart(fig)

# plot_users_by_brand_top_states(aggr_user):
def plot_users_by_brand_top_states(aggr_user):
    st.subheader("Number of Users by Phone Brand in Top States")

    # Step 1: Get top states by total user count
    state_list = aggr_user.groupby('state')['count'].sum().sort_values(ascending=False)
    top_states = state_list.index.tolist()

    # Step 2: Group by state and brand
    df = aggr_user.groupby(['state', 'brand'])['count'].sum().reset_index()
    df = df[df['state'].isin(top_states)]
    df['state'] = pd.Categorical(df['state'], categories=top_states, ordered=True)

    # Step 3: Plot as stacked bar chart
    fig = px.bar(
        df,
        x='state',
        y='count',
        color='brand',
        title='Number of Users by Phone Brand Across Top States',
        barmode='stack',
        color_discrete_sequence=px.colors.qualitative.Bold,
        height=600
    )
    fig.update_layout(
        xaxis_title='state',
        yaxis_title='User Count',
        xaxis_tickangle=45
    )
    st.plotly_chart(fig)

# plot_insurance_aggregates(aggr_insurance)
def plot_insurance_aggregates(aggr_insurance):
    st.title("Insurance Aggregates - Clustered Bar Charts")

    # Step 1: Top 20 states by total insurance amount
    sorted_states = (
        aggr_insurance.groupby('state')['amount']
        .sum()
        .sort_values(ascending=False)
        .head(20)
        .index
        .tolist()
    )

    st.subheader("Insurance Amount by state and Year (Clustered)")

    chart1 = (
        aggr_insurance.groupby(['state', 'year'])['amount']
        .sum()
        .reset_index()
    )
    chart1 = chart1[chart1['state'].isin(sorted_states)]
    chart1['state'] = pd.Categorical(chart1['state'], categories=sorted_states, ordered=True)

    fig_year = px.bar(
        chart1,
        x='state',
        y='amount',
        color='year',
        barmode='group',
        title='Insurance Amount by state and Year',
        color_discrete_sequence=px.colors.sequential.Viridis,
        height=600
    )
    fig_year.update_layout(
        xaxis_title='state',
        yaxis_title='Insurance Amount',
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_year)

    st.subheader("Insurance Amount by state and Quarter (Clustered)")

    chart2 = (
        aggr_insurance.groupby(['state', 'quater'])['amount']
        .sum()
        .reset_index()
    )
    chart2 = chart2[chart2['state'].isin(sorted_states)]
    chart2['state'] = pd.Categorical(chart2['state'], categories=sorted_states, ordered=True)

    fig_quarter = px.bar(
        chart2,
        x='state',
        y='amount',
        color='quater',
        title='Insurance Amount by state and Quarter',
        color_discrete_sequence=px.colors.sequential.Viridis,
        height=600
    )
    fig_quarter.update_layout(
        xaxis_title='state',
        yaxis_title='Insurance Amount',
        xaxis_tickangle=45
    )
    st.plotly_chart(fig_quarter)

#  plot_transaction_count_by_type(aggr_trans):
def plot_transaction_count_by_type(aggr_trans):
    st.subheader("Transaction Count by state Across Transaction Types (Top 20 States)")

    # Step 1: Aggregate and get top 20 states
    sorted_states = (
        aggr_trans.groupby('state')['Transaction_count']
        .sum()
        .sort_values(ascending=False)
        .head(20)
        .index
        .tolist()
    )

    # Step 2: Prepare grouped DataFrame
    chart_data = (
        aggr_trans.groupby(['state', 'Transaction_type'])['Transaction_count']
        .sum()
        .reset_index()
    )
    chart_data = chart_data[chart_data['state'].isin(sorted_states)]
    chart_data['state'] = pd.Categorical(chart_data['state'], categories=sorted_states, ordered=True)

    # Step 3: Plot
    fig = px.bar(
        chart_data,
        x='state',
        y='Transaction_count',
        color='Transaction_type',
        barmode='group',
        title='Transaction Count by state Across Transaction Types',
        color_discrete_sequence=px.colors.qualitative.Set2,
        height=600
    )
    fig.update_layout(
        xaxis_tickangle=45,
        xaxis_title='state',
        yaxis_title='Transaction Count'
    )
    st.plotly_chart(fig)


# plot_largest_smallest_districts(map_transaction):
def plot_largest_smallest_districts(map_transaction):
    st.subheader("Largest vs Smallest District Transaction Amount by state")

    # Drop 'type' column if exists
    df2 = map_transaction.drop(['type'], axis=1, errors='ignore')
    
    # Group by state and district
    agg = df2.groupby(['state', 'dist_name'])['ammount'].sum().reset_index()

    # Largest and smallest district in each state
    largest = agg.groupby('state', group_keys=False).apply(lambda x: x.nlargest(1, 'ammount'))
    smallest = agg.groupby('state', group_keys=False).apply(lambda x: x.nsmallest(1, 'ammount'))

    largest['type'] = 'Largest'
    smallest['type'] = 'Smallest'

    combined = pd.concat([largest, smallest])
    combined['label'] = combined['state'] + ' - ' + combined['dist_name']

    # Create pivot table for bar chart
    pivot = combined.pivot(index='label', columns='type', values='ammount').fillna(0).sort_values(by='Largest', ascending=True)

    # Plotly Horizontal Bar Chart
    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=pivot.index,
        x=pivot['Largest'],
        name='Largest',
        orientation='h',
        marker_color='green'
    ))

    fig.add_trace(go.Bar(
        y=pivot.index,
        x=pivot['Smallest'],
        name='Smallest',
        orientation='h',
        marker_color='orange'
    ))

    fig.update_layout(
        barmode='group',
        title='Largest vs Smallest District Transaction Amount by state',
        xaxis_title='Transaction Amount',
        yaxis_title='state - District',
        height=1200
    )
    st.plotly_chart(fig)

# table user registration with pin code
def user_registration_analysis(top_user):
    path= r"G:\python\New folder\pincode.csv"     # pincodes in india file csv
    df=pd.read_csv(path)
    df.drop(['CircleName', 'RegionName', 'DivisionName', 'StateName','OfficeName','OfficeType', 'Delivery', 'Latitude','Longitude'],axis=1,inplace=True)
    df['pincode']=df['Pincode']
    df.drop(['Pincode'],axis=1,inplace=True)

    com=pd.merge(top_user,df,on='pincode',how='inner')
    com=com.drop_duplicates()
    result = com.groupby(['state', 'District', 'pincode'])['registeredUsers'].sum().reset_index()
    result = result.sort_values(by=['state', 'registeredUsers'], ascending=[True, False])
    st.dataframe(result)

#((((((((((((((((((((((((((((((((((((((((((((((            ))))))))))))))))))))))))))))))))))))))))))))))

# data insight function 
def aggr_transaction_datainsight(df, year, quarter, state,transaction_type):
    df = state_name_order(df)

    if year != "All":
        df = df[df['Year'] == year]
    if quarter != "All":
        df = df[df['Quarter'] == quarter]
    if state != "All":
        df = df[df['state'] == state]
    if transaction_type != 'All':
        df = df[df['Transaction_type']==transaction_type]

    df.reset_index(inplace=True)

    fig_amount = px.bar(df, x="state",y="Transaction_amount",title=f"{year}TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
    st.plotly_chart(fig_amount)
    
    fig_count = px.bar(df, x="state",y="Transaction_count",title=f"{year}TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered_r,height=650,width=650)
    st.plotly_chart(fig_count)

    # Load GeoJSON
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)

    # Extract state names
    state = [feature["properties"]["ST_NM"] for feature in data["features"]]
    state.sort()
    
    
    df1 = df.groupby(['state'])['Transaction_amount'].sum().reset_index()
    
    fig_india_1 = px.choropleth(
        df1,
        geojson=data,
        locations="state",
        featureidkey="properties.ST_NM",
        color="Transaction_amount",
        color_continuous_scale="Rainbow",
        range_color=(df1["Transaction_amount"].min(), df1["Transaction_amount"].max()),
        hover_name="state",
        title = f"{year if year != 'All' else 'All Years'} - {quarter if quarter != 'All' else 'All Quarters'} - {transaction_type if transaction_type != 'All' else 'All Types'} Transaction Amount",
        fitbounds="locations",
        height=600,
        width=600
    )

    fig_india_1.update_geos(visible=True)
    st.plotly_chart(fig_india_1)

    df2=df.groupby(['state'])['Transaction_count'].sum().reset_index()
    fig_india_2 = px.choropleth(
        df2,
        geojson=data,
        locations='state',
        featureidkey='properties.ST_NM',
        color="Transaction_count",
        color_continuous_scale="Rainbow",
        range_color=(df2["Transaction_count"].min(), df2["Transaction_count"].max()),
        hover_name="state",
        title = f"{year if year != 'All' else 'All Years'} - {quarter if quarter != 'All' else 'All Quarters'} - {transaction_type if transaction_type != 'All' else 'All Types'} Transaction Amount",
        fitbounds="locations",
        height=600,
        width=600
    )
    st.plotly_chart(fig_india_2)

def table_aggr_user(df, year, quarter, state,brand):
    df = state_name_order(df)

    if year != "All":
        df = df[df['Year'] == year]
    if quarter != "All":
        df = df[df['Quarter'] == quarter]
    if state != "All":
        df = df[df['state'] == state]
    if brand != 'All':
        df = df[df['brand']==brand]

    df.reset_index(inplace=True)
    bar_df=df.groupby(['brand'])['count'].sum().reset_index()

    fig_amount = px.bar(bar_df, x="brand",y="count",title=f"{year}COUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
    st.plotly_chart(fig_amount)
    
    
    # Load GeoJSON
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)

    # Extract state names
    state = [feature["properties"]["ST_NM"] for feature in data["features"]]
    state.sort()
    
    
    df1 = df.groupby(['state'])['count'].sum().reset_index()
    
    fig_india_1 = px.choropleth(
        df1,
        geojson=data,
        locations="state",
        featureidkey="properties.ST_NM",
        color="count",
        color_continuous_scale="Rainbow",
        range_color=(df1["count"].min(), df1["count"].max()),
        hover_name="state",
        title = f"{year if year != 'All' else 'All Years'} - {quarter if quarter != 'All' else 'All Quarters'} - {brand if brand != 'All' else 'All Types'} COUNT ",
        fitbounds="locations",
        height=600,
        width=600
    )

    fig_india_1.update_geos(visible=True)
    st.plotly_chart(fig_india_1)

def  map_transaction_analysis_1(df,year):
    df = state_name_order(df)
    # Load GeoJSON
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)

    # Extract state names
    state = [feature["properties"]["ST_NM"] for feature in data["features"]]
    state.sort()

    fig_df=df.groupby(['state'])[['count','ammount']].sum().reset_index()
    fig_india_1 = px.choropleth(
        fig_df,
        geojson=data,
        locations='state',
        featureidkey='properties.ST_NM',
        color="count",
        color_continuous_scale="Rainbow",
        range_color=(fig_df["count"].min(), fig_df["count"].max()),
        hover_name="state",
        title = f"{year if year != 'All' else 'All Years'} - ALL STATE",
        fitbounds="locations",
        height=600,
        width=600
    )
    st.plotly_chart(fig_india_1)



    bar_df=df[df['year'].astype(int) == year].groupby(['state'])[['count','ammount']].sum().reset_index()

    col1,col2=st.columns(2)

    with col1:
        
        map_fig_amount = px.bar(bar_df, x="state",
                                y="ammount",
                                title=f"{years} MAP TRANSACTION AMOUNT",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
        
        st.plotly_chart(map_fig_amount)
    with col2:
        
        map_fig_count = px.bar(bar_df, x="state",
                                y="count",
                                title=f"{years} MAP TRANSACTION COUNT",
                                color_discrete_sequence=px.colors.sequential.Bluered_r,height=650,width=650)
        st.plotly_chart(map_fig_count)

def map_transaction_analysis_2(map_transaction,state,year):
    df=map_transaction
    df=df[df['year']==year]
    if state != "All":
        df=df[df['state']==state]
    
    df1=map_transaction[map_transaction['state']==state].groupby(['dist_name'])[['count','ammount']].sum().reset_index()
    col1,col2=st.columns(2)
    with col1:
        fig_1=px.bar(df1, x="dist_name",
                               y="count",
                               title=f"{year} MAP TRANSACTION COUNT",
                               color_discrete_sequence=px.colors.sequential.Bluered_r,height=650,width=650)
        st.plotly_chart(fig_1)
    with col2:
        fig_2=px.bar(df1, x="dist_name",
                               y="ammount",
                               title=f"{year} MAP TRANSACTION AMOUNT",
                               color_discrete_sequence=px.colors.sequential.Bluered_r,height=650,width=650)
        st.plotly_chart(fig_2)

def map_user_analysis_1(map_user,year):
    bar_df=map_user
    state_name_order(map_user)
    bar_df=bar_df[bar_df['year']==year]
    bar_df=bar_df.groupby(['state'])['registeredUsers'].sum().reset_index()
    fig_bar=px.bar(bar_df,x='state',y='registeredUsers',title=f"{year}-Registered Users",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
    st.plotly_chart(fig_bar)

    # Load GeoJSON
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)

    # Extract state names
    state = [feature["properties"]["ST_NM"] for feature in data["features"]]
    state.sort()
    
    fig_df = map_user.groupby(['state'])['registeredUsers'].sum().reset_index()
    
    fig_india_1 = px.choropleth(
        fig_df,
        geojson=data,
        locations="state",
        featureidkey="properties.ST_NM",
        color="registeredUsers",
        color_continuous_scale="Rainbow",
        range_color=(fig_df["registeredUsers"].min(), fig_df["registeredUsers"].max()),
        hover_name="state",
        title = f"{year if year != 'All' else 'All Years'} Registered Users",
        fitbounds="locations",
        height=600,
        width=600
    )

    fig_india_1.update_geos(visible=True)
    st.plotly_chart(fig_india_1)

def map_user_analysis_2(map_user,year,state):
    bar_df=map_user[map_user['year']==year]
    
    if state != "All":
        bar_df=bar_df[bar_df['state']==state]

    col1,col2=st.columns(2)
    with col1:
        chart_df=bar_df.groupby(['dist_name'])['registeredUsers'].sum().reset_index()
        fig_bar=px.bar(chart_df,x='dist_name',y='registeredUsers',title=f"{year}-Registered Users",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
        st.plotly_chart(fig_bar)
    with col2:
        chart_df_2=bar_df.groupby(['dist_name'])['appOpen'].sum().reset_index()
        fig_bar=px.bar(chart_df_2,x='dist_name',y='appOpen',title=f"{year}-appOpen",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
        st.plotly_chart(fig_bar)

def insurance_analysis_1(aggr_insurance,year):

    map_df=aggr_insurance[aggr_insurance['year']==year]
    
    # Load GeoJSON
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)

    # Extract state names
    state = [feature["properties"]["ST_NM"] for feature in data["features"]]
    state.sort()
    fig_df=state_name_order(map_df)
    fig_df = map_df.groupby(['state'])['count'].sum().reset_index()
    
    fig_india_1 = px.choropleth(
        fig_df,
        geojson=data,
        locations="state",
        featureidkey="properties.ST_NM",
        color="count",
        color_continuous_scale="Rainbow",
        range_color=(fig_df["count"].min(), fig_df["count"].max()),
        hover_name="state",
        title = f"{year if year != 'All' else 'All Years'} COUNT",
        fitbounds="locations",
        height=600,
        width=600
    )

    #fig_india_1.update_geos(visible=True)
    st.plotly_chart(fig_india_1)
    
    col1,col2=st.columns(2)
    with col1:
        map_df1=map_df.groupby('state')['count'].sum().reset_index()
        fig_bar=px.bar(map_df1,x='state',y='count',title=f"{year}- COUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
        st.plotly_chart(fig_bar)
    with col2:
        map_df2=map_df.groupby('state')['amount'].sum().reset_index()
        fig_bar=px.bar(map_df2,x='state',y='amount',title=f"{year}- AMOUNT ",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=650)
        st.plotly_chart(fig_bar)

#(((((((((((((((((((((((((((((((((((((((((((((((((((((((())))))))))))))))))))))))))))))))))))))))))))))))))))))))

# Streamlit UI

st.set_page_config(page_title="PhonePe Data", layout="wide")

with st.sidebar:
    select = option_menu("menu", ["Phone pay","Data Insights", "Performance Charts"])
if select == "Phone pay":
    st.header('phone pay digital payment')
    st.markdown("""
                    ## 📊 About the Project

                    Welcome to the **PhonePe Data Visualization Project**!  
                    This dashboard presents an interactive and insightful exploration of **digital payment trends in India** using the official PhonePe Pulse data.  
                    We aim to understand user behavior, transaction patterns, geographic variations, and the growth of digital payments across different Indian states and districts.

                    ---

                    ## 🔍 Data Insights

                    In the **Data Insights** section, you'll find detailed visualizations and analyses categorized into:
                    - **Aggregated Analysis**: Trends over time in transaction counts and values.
                    - **Map Analysis**: State-wise and district-wise digital payment penetration.
                    - **Insurance Analysis**: Insights into digital insurance usage and patterns.

                    ---

                    ## 👤 About the Author

                    This project is developed by **Aravind**, With this dashboard, the goal is to make complex data easy to understand and useful for Bussiness insights and future plannings.

                    ---

                    ## Performance Chart
                    - Aggregate Transaction Input
                    - Device Dominance and User Engagement
                    - Insurance Penetration and Growth Potential
                    - Transaction Analysis for Market Expansion
                    - User Registration Analysis

                    """)
elif select == "Data Insights":
    tab1, tab2, tab3 = st.tabs(["Aggregated Analysis", "Map Analysis", "Insurance Analysis"])

    with tab1:
        select = st.radio("Select ", ["Aggregated Transaction Analysis", "Aggregated User Analysis"])

        if select == "Aggregated Transaction Analysis":
            year_options = ["All"] + sorted(aggr_trans["Year"].unique().tolist())
            year = st.selectbox("Select the Year", year_options)

            quarter_options = ["All"] + sorted(aggr_trans["Quarter"].unique().tolist())
            quarter = st.selectbox("Select the Quarter", quarter_options)

            state_options = ["All"] + sorted(aggr_trans['state'].unique().tolist())
            state = st.selectbox("Select state", state_options)

            transaction_type_options=["All"]+sorted(aggr_trans['Transaction_type'].unique().tolist())
            transaction_type=st.selectbox("select Transaction Type",transaction_type_options)

            if st.button("Go"):
                aggr_transaction_datainsight_1(aggr_trans, year, quarter, state,transaction_type)
                
        elif select == "Aggregated User Analysis":
            year_options = ["All"] + sorted(aggr_user["year"].unique().tolist())
            year = st.selectbox("Select the Year", year_options)

            quarter_options = ["All"] + sorted(aggr_user["quater"].unique().tolist())
            quarter = st.selectbox("Select the Quarter", quarter_options)

            state_options = ["All"] + sorted(aggr_user['state'].unique().tolist())
            state = st.selectbox("Select state", state_options)

            brand_option = ["All"] + sorted ( aggr_user["brand"].unique().tolist())
            brand = st.selectbox("select brand",brand_option )

            if st.button("Go"):
                aggr_transaction_datainsight_2(aggr_user,year,state,quarter,brand)
    
    with tab2:
        select = st.radio("Select the Method", ["Map Transaction Analysis", "Map User Analysis"])
        if select == "Map Transaction Analysis":
            
            years=st.slider("select the year",int(map_transaction['year'].min()),int(map_transaction['year'].max()),int(map_transaction['year'].min()))
            map_transaction_analysis_1(map_transaction,years)
            
            state_options= ["All"] + sorted (map_transaction['state'].unique().tolist())
            state=st.selectbox("select the state",state_options)
            map_transaction_analysis_2(map_transaction,state,years)
        
        elif select == "Map User Analysis":

            year=st.slider("select the year",int(map_user['year'].min()),int(map_user['year'].max()),int(map_user['year'].min()))
            map_user_analysis_1(map_user,year)
            state_option = ['All']+sorted (map_user['state'].unique().tolist())
            state=st.selectbox("select the state",state_option)
            map_user_analysis_2(map_user,year,state)

    with tab3:
        year = st.slider("select the year",int(aggr_insurance['year'].min()),int(aggr_insurance['year'].max()),int(aggr_insurance['year'].min()))
        insurance_analysis_1(aggr_insurance,year)

elif select=="Performance Charts":
    charts = st.selectbox(
        "Select the performance chart",
        [
            "Aggregate Transaction Input",
            "Device Dominance and User Engagement",
            "Insurance Penetration and Growth Potential",
            "Transaction Analysis for Market Expansion",
            "User Registration Analysis"
        ]
    )


    if charts == "Aggregate Transaction Input":
        st.subheader("Aggregate Transaction Input")
        plot_aggregated_transaction_pie_charts(aggr_trans)
        plot_top_transaction_analysis(aggr_trans)
        plot_transaction_clustered_bars(aggr_trans)

    elif charts == "Device Dominance and User Engagement":
        st.subheader("Device Dominance and User Engagement")
        plot_top_phone_brands(aggr_user)
        plot_users_by_brand_top_states(aggr_user)


    elif charts == "Insurance Penetration and Growth Potential":
        st.subheader("Insurance Penetration and Grouwth Potential")
        plot_insurance_aggregates(aggr_insurance)
        

    elif charts== "Transaction Analysis for Market Expansion":
        st.subheader("Transaction Analysis for Market Expansion")
        plot_transaction_count_by_type(aggr_trans)
        plot_largest_smallest_districts(map_transaction)

    elif charts== "User Registration Analysis":
        st.subheader("User Registration Analysis")
        user_registration_analysis(top_user)
