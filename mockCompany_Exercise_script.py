
import json
import psycopg2
import getpass
import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', 20)


server = 'na'
username = 'readonly'
port = '5432'
db = 'test'


def get_data(server,username,port,db):
    pw = getpass.getpass('Database Password: ') #no pws in code!


    try:
        conn = psycopg2.connect(host=server,port=port,database=db, user=username, password=pw)
        if conn:
            print('Now Connected to DB')
    except (Exception, psycopg2.Error) as e :
        print ("Could not connect to DB", e)

    cursor = conn.cursor()

    #Tap SQL
    query = """
    SELECT * FROM assignment
    """
    cursor.execute(query)
    data = pd.read_sql(query, conn)
    print(data.head())
    
    #A little cleanup and parsing
    json_struct = json.loads(data.to_json(orient="records"))    
    data = pd.json_normalize(json_struct)
    print(data.head())
    data.drop(labels='properties', axis=1, inplace=True)
    data.rename(columns={
        'properties.first_os_type': 'first_os', 
        'properties.first_device_type': 'first_device', 
        'properties.first_browser_type': 'first_browser'}, inplace=True)
    data['first_os'] = data['first_os'].str[0]
    data['first_device'] = data['first_device'].str[0]
    data['first_browser'] = data['first_browser'].str[0]
    data['created_at'] = pd.to_datetime(data['created_at'], unit='ms')

    conn.close()
    print("Connection Closed")
    return(data)


#Pull data and take a look at it. Get familiar with features and characteristics
df = get_data(server,username,port,db)

print(df.head(20))
print(df.dtypes,"\n")
df = df.drop_duplicates()

#Add Some date features for trending analysis (NOT USED DUE TO TIME CONSTRAINTS)
df['date'] = df['created_at'].dt.date
df['month'] = df['created_at'].dt.month_name()
df['weekday'] = df['created_at'].dt.day_name()
df['tod'] = pd.cut(
    df['created_at'].dt.hour, bins=[0,4,8,12,16,20,24], labels=['Late Night', 'Early Morning','Morning','Noon','Eve','Night'], include_lowest=True)
print(df.head())

#Run Correlation analysis
et = df.groupby(['date','event_type'])['id'].count().unstack()
join_add_corr = et.corr()
print(join_add_corr)

etu = df.groupby(['date','event_type','user_id'])['id'].count()
#etu.to_csv('etu.csv', index=True)

#split into the 2 types of events for easier aggregations
dfc = df[df['event_type'] == 'added client']
dfu = df[df['event_type'] == 'user created']

print(len(dfc))

#Basic Exploratory Analysis 
nrow = df.shape[0]
print("total events", nrow)

# Lets get some counts for analysis
c_counts = dfc['user_id'].agg(['count', 'nunique'])
u_counts = dfu['user_id'].agg(['count', 'nunique'])
c_user =dfc.groupby('user_id')['event_type'].agg(['count']).sort_values(by='count', ascending= False)

#Timing analysis
add_activity = pd.merge(
    dfu[['user_id','created_at','first_os','first_device','first_browser']],
    dfc[['date','user_id','created_at']],
    how='right',on=['user_id']).drop_duplicates()
add_activity.rename(columns={'created_at_x':'join_ts', 'created_at_y':'add_client_ts'}, inplace=True)
add_activity['diff'] = (add_activity['add_client_ts'] - add_activity['join_ts']).dt.days
add_activity.to_csv('mergy.csv', index=False)




# Date Master (TBD: NOT USED IN THIS ANALYSIS DUE TO TO TIME CONSTRAINTS)
start = df['created_at'].min()
end = df['created_at'].max()
d = pd.date_range(start, end, freq="1H", normalize=True).to_frame(index=False, name="ts")

