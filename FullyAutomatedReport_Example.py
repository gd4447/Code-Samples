import json
import requests
import pandas as pd
import xlsxwriter
import numpy as np
import pyodbc
import getpass
import datetime


"""
Created on Fri Jan 18 11:24:30 2019

@author: Gerardo

This script generates an "Activity Report" that some clients have requested on a weekly basis. 
All you need to do is run ithe script and all the reports will appear on your specified path.

"""

#WEB PARAMETERS
user = 'my username'

#SQL PARAMETERS - Automatically pick Monday thru Sunday
username = "another username"
today = datetime.date.today()
last_mon = today - datetime.timedelta(days=today.weekday(), weeks=1)
last_sun = today - datetime.timedelta(days=today.weekday() + 1)

start = last_mon.strftime("%Y-%m-%d")
end = last_sun.strftime("%Y-%m-%d")

#Clients that have requested the report
cmap = {
    #map containing (client:db name) pairs
    }

#obtain Token for API
def get_token(env, user=user,auth='user'):         

        pw = getpass.getpass('Please enter password for {}.{} : '.format(account, accountValue))

        url = 'https://someurl'
        payload ={'grant_type':'password', 'username': user ,'password': pw}
        headers = {'content-type':'applievention/json'}
            
        token = json.loads(requests.post(url, payload, headers).text)['access_token']
        auth_header = {'Authorization': 'Bearer {}'.format(token), 'content-type':'application/json'}
        return(auth_header)


#GET LIST OF NAMED USERS via API
def get_users(env,token):
    url = 'https://someurl'
    users = json.loads(requests.get(url, headers=token).text)['results']
    
    users = pd.DataFrame(users).set_index('id')
    users['accountType'] = np.where((users['iseventAdmin'] == True) & (users['isAccountAdmin'] == True), 'Admin', 'User')
  
    users.columns = pd.MultiIndex.from_product([['user_data'], users.columns])
    users.index = users.index.astype(str)
    users.rename_axis(None)
   
    return(users)


#GET LIST OF NAMED events via API
def get_events(env,token):
    url = 'https://someurl'
    events = json.loads(requests.get(url, headers=token).text)['results']
    
    events = pd.DataFrame(events).set_index('id')
    events = events[['displayName']]
    events.columns = pd.MultiIndex.from_product([['event_data'], events.columns])
    
    return(events)


#SQL Setup & Query
def get_data(start,end,client,username,pw):
    server = "someserver"
    db = "somedb"
    driver = '{ODBC Driver 17 for SQL Server}'

    conn = pyodbc.connect(
    'DRIVER='+driver+
    ';PORT=someport;SERVER='+server+
    ';PORT=someport;DATABASE='+db+
    ';UID='+username+
    ';PWD='+pw+
    ';Authentievention=ActiveDirectoryPassword')

    print('Extracting data...',conn)

    records = """
    SELECT
    a.UserId,
    u.UserId as UserIdMap,
    a.DatetimeUtc,
    a.DateUtc,
    e.recordsEventType,
    s.recordsSubType,
    a.[Values],  
    a.TargetIdMap,
    acc.[Name] as Account
    FROM [dbo].[records] a
    INNER JOIN dbo.Account acc ON a.CustomerId = acc.AccountId
    INNER JOIN dbo.recordsEventType e ON a.EventType = e.recordsEventTypeId
    INNER JOIN dbo.recordsSubType s ON a.SubType = s.recordsSubTypeId
    INNER JOIN dbo.recordsUser u ON a.UserId = u.recordsUserId
    WHERE a.DateUtc BETWEEN '{}' AND '{}'
    AND e.recordsEventType IN (Event List (Anonimyzed))
    AND s.recordsSubType IN ('SubType list (Anonymized)
    AND acc.Name = '{}';""".format(start,end,client)
  
  
    data = pd.read_sql(records, conn)
    conn.close()
    return(data)


#CLEAN UP SQL QUERY DATA
def process_records(engagement_data):
    df = engagement_data
    df['total_count'] = np.arange(len(df))
    df['TargetIdMap'] = df['TargetIdMap'].str.split(',', n=1).str[0]
    
    df[['target_type','target']]= df['TargetIdMap'].str.split('/', n=-1,expand=True)
    df[['user_type','user']]= df['UserIdMap'].str.split('/', n=-1,expand=True)

    #Change Names of Confusing stuff
    df['recordsEventType']=df['recordsEventType'].replace({'ChartReports':'Report','ConditionaleventModel':'event', 'ScriptClassifier', 'event'})

    return(df)



#Combining some data points
def user_processing(clean_records):
    db = clean_records.groupby(['unique_user_count','event'])['target'].agg(['count','nunique']).unstack()
     
    return(db)


def call_processing(clean_records):
    db = clean_records.groupby('target_type')
    c = db.get_group('c').groupby(['target','event'])[['total_count','unique_user_count']].agg('nunique').unstack()
    
    return(c)
    
    
def event_processing(clean_records):
    db = clean_records.groupby('target_type')
    ev = db.get_group('cls').groupby(['target','event'])[['total_count','unique_user_count']].agg('nunique').unstack()

    return(ev)


#RUN/COORDINATE ALL FUNCTIONS and WRITE TO EXCEL
if __name__ == '__main__':

    pw = getpass.getpass('SSMS Password: ')
    
    for i in cmap:
        token = get_token(i)
        users = get_users(i,token)
        events = get_events(i,token)
        data = get_engagement_data(start,end,cmap[i],username,pw)

        cleanedData = process_records(data)

        #USER page
        u = user_records(cleanedData)
        u = pd.merge(users, u, left_on='id', right_index=True, how='left')
        u[('user_data','totalActivity')] = u.sum(axis=1,level=0)['count'] #can swap for nunique
        u = u.sort_values([('user_data','totalActivity')], ascending=False)
        u.rename({'nunique':'unique_count'},axis=1, level=0,inplace=True)
   

        #CALLS page
        c = call_records(cleanedData)
        c.index.name = 'callId'
  

        #EVENTS page
        event = event_records(cleanedData)
        event = pd.merge(events, event, left_on='id', right_index=True, how='right')
        event[('event_data','displayName')].replace(np.nan, 'This event is no longer active', inplace=True)



        #EXCEL STUFF
        writer = pd.ExcelWriter('ReportName.xlsx'.format(i,last_mon.strftime('%m%d'),last_sun.strftime('%m%d')),engine='xlsxwriter')

        try:
            u.to_excel(writer, sheet_name='UserActivity',index=True,freeze_panes=(2,0))
            userActivity = writer.sheets['UserActivity']
            userActivity.set_column('B:AK', 17)
        except Exception as e:
            print("An exception occurred in Users: ",e)
            pass
    
        try:
            c.to_excel(writer, sheet_name='CallActivity',index=True,freeze_panes=(2,0))
            callActivity = writer.sheets['CallActivity']
            callActivity.set_column('A:G', 16)
        except Exception as e:
            print("An exception occurred in calls: ",e)
            pass

        try:
            event.to_excel(writer, sheet_name='eventActivity',index=True,freeze_panes=(2,0))
            eventActivity = writer.sheets['eventActivity']
            eventActivity.set_column('B:W', 16)
            eventActivity.set_column('A:A', None,None, {'hidden': True})
        except Exception as e:
            print("An exception occurred in events: ",e)
            pass

        writer.save()