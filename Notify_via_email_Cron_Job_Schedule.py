import os
import smtplib
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import datetime

# ESTABLISHING CONNECTION WITH THE SNOWFLAKE WAREHOUSE 
engine = create_engine(URL(
   account='nua76068.us-east-1',
   user='BURHAND',
   password='xxxxxxxxxxxxx',
   database='AB_INTENT_KW',
   schema='INBOUND',
   warehouse='COMPUTE_WH',
   role='SYSADMIN',
))


#send mail
def send_mail(df,df1,df2,df3):
    msg = MIMEMultipart()
    msg['Subject'] = 'Audience Bridge QC'
    msg['From'] = 'burhan.din@transmissionagency.com'
    msg['To'] = ("burhan.din@transmissionagency.com,Neelkamal.sahu@transmissionagency.com,claire.gardner@transmissionagency.com")
    
    df3['updated_date'] = pd.to_datetime(df3['updated_date'])
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    
    # Find the date of the most recent Sunday
    today = datetime.datetime.today()
    last_sunday = today - datetime.timedelta(days=today.weekday()+1)

    # Check if the date in the DataFrame is equal to last Sunday's date
    updated_date_str = df3['updated_date'].dt.strftime("%Y-%m-%d").iloc[0]
    updated_at = df['updated_at'].max() 
    today1 = datetime.datetime.today().date()
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.date()

    #if we don't have the latest data present in AB_RAW Table then if block will get executed and will send us a text message
    if updated_date_str != last_sunday.date().strftime("%Y-%m-%d") and updated_at.date() != today1 and updated_at.date() != yesterday:
        body_text4 = f'We do not have the latest data present till {last_sunday.date().strftime("%Y-%m-%d")}. The data is present till {updated_date_str} as the client has not uploaded the data this week'
    elif (updated_date_str != last_sunday.date().strftime("%Y-%m-%d")) and (updated_at.date() == today1 or updated_at.date() == yesterday):
    # This else block will get executed if the client uploaded the data and if we  get the data in the AB_RAW Table
    # Set the email body text to include the dataframe information
        if len(df) > 0:
            body_text1 = f'The Client has uploaded the data which has been loaded successfully on {updated_at}.\n The total Number of Rows loaded = {df["rows_loaded"][0]}\n Date Range: {df["min_date"][0]} to {df["max_date"][0]}\n The data is not the latest i.e., till {last_sunday.date().strftime("%Y-%m-%d")} \n'
        else:
            body_text1 = ''
            
        # Create an empty list to store the countries with missing data
        countries_missing_data = []

        # Loop through each row of the data frame
        for index, row in df1.iterrows():
            countries_missing_data.append(row['country'])
        if len(countries_missing_data) > 0:
            body_text2 = 'The Countries that we didn''t get data for are: {}'.format(", ".join(countries_missing_data))

        else:
            # If all the countries have data available, set the message body accordingly
            body_text2 = 'The Data contains all the 15 Countries\n\n'

        # Calculate the percentage of rows with respect to the total number of rows in the table
        total_rows = len(df2)
        percentage = total_rows / 843 * 100
        percentage = 100 - percentage
        low_percentage = total_rows / 843 * 100
        
        # Set the email body text to include the percentage information
        if percentage >= 90:
            body_text3 = f'The Keyword match score is {percentage:.2f}% '
        else:
            body_text3 = f'The non-matched keywords percentage is {low_percentage:.2f}% . Please find the attachment'
            
            # Attach the dataframe as a CSV file
            with open (ext_file_location,'rb') as f:
                file_name = os.path.basename(ext_file_location)
                msg.attach(MIMEApplication(f.read(), Name=file_name))
    else:
    # This else block will get executed if we have got the latest data present in the AB_RAW Table
    # Set the email body text to include the dataframe information
        if len(df) > 0:
            body_text1 = f'The latest Data has been loaded successfully on {updated_at}.\n The total Number of Rows loaded = {df["rows_loaded"][0]}\n Date Range: {df["min_date"][0]} to {df["max_date"][0]}\n and is upto date till {last_sunday.date().strftime("%Y-%m-%d")}\n'
        else:
            body_text1 = 'No data found for the first dataframe'
            
        # Create an empty list to store the countries with missing data
        countries_missing_data = []

        # Loop through each row of the data frame
        for index, row in df1.iterrows():
            countries_missing_data.append(row['country'])
        if len(countries_missing_data) > 0:
            body_text2 = 'The Countries that we didn''t get data for are: {}'.format(", ".join(countries_missing_data))

        else:
            # If all the countries have data available, set the message body accordingly
            body_text2 = 'The Data contains all the 15 Countries\n\n'

        # Calculate the percentage of rows with respect to the total number of rows in the table
        total_rows = len(df2)
        percentage = total_rows / 843 * 100
        percentage = 100 - percentage
        low_percentage = total_rows / 843 * 100
        
        # Set the email body text to include the percentage information
        if percentage >= 90:
            body_text3 = f'The Keyword match score is {percentage:.2f}% '
        else:
            body_text3 = f'The non-matched keywords percentage is {low_percentage:.2f}% . Please find the attachment'
            
            # Attach the dataframe as a CSV file
            with open (ext_file_location,'rb') as f:
                file_name = os.path.basename(ext_file_location)
                msg.attach(MIMEApplication(f.read(), Name=file_name))

    if updated_date_str != last_sunday.date().strftime("%Y-%m-%d") and updated_at.date() != today1 and updated_at.date() != yesterday:
        body_text5 = f'{body_text4}\n'
    elif (updated_date_str != last_sunday.date().strftime("%Y-%m-%d")) and (updated_at.date() == today1 or updated_at.date() == yesterday):
        body_text5 = f'{body_text1}\n{body_text2}\n{body_text3}\n'
    else: 
        body_text5 = f'{body_text1}\n{body_text2}\n{body_text3}\n' 

    body_text = f'Hi Team, \n\n  {body_text5}\n\nRegards,\nBurhan ud din'    

    msg.attach(MIMEText(body_text))
            

    with smtplib.SMTP('smtp.office365.com',587) as smtp:
        smtp.starttls()
        smtp.login('burhan.din@transmissionagency.com', 'xxxxxxxxxxxxxxxxxxxxxx')
        smtp.send_message(msg)
        print('Mail sent')

 
# FETCHING THE DATA FROM SNOWFLAKE USING SQL AND STORING IT IN A VARIABLE

sql1='''
select * from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1;
'''

sql2='''
select COUNTRY from AB_INTENT_KW.INBOUND.COUNTRY WHERE COUNTRY NOT IN (
select COUNTRYCODE From AB_INTENT_KW.INBOUND.AB_RAW where COUNTRYCODE IS NOT NULL 
AND date(timestamp) BETWEEN (select MIN_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1)
AND  (select MAX_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1));
''' 
sql3 = '''
select KEYWORD from "AB_INTENT_KW"."INBOUND"."TOPICS_MAPPING" where CLIENT = 'Maersk' AND KEYWORD NOT IN 
(select distinct replace(KWTEXT,'_',' ') AS KEYWORDTEXT from AB_INTENT_KW.INBOUND.AB_RAW
where date(timestamp) BETWEEN (select MIN_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1)
AND  (select MAX_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1))
'''

sql4='''
select  max(date(timestamp)) as updated_date from AB_INTENT_KW.INBOUND.AB_RAW;
'''

# CONVERTING THE FETCHED DATA INTO PANDAS DATAFRAME FOR EASY MANIPULATIONS AND WRITING TO FILE
df = pd.read_sql(sql1, engine)
#print(df)
df1 = pd.read_sql(sql2, engine)
#print (df1)
df2 = pd.read_sql(sql3, engine)
#print (df2)
df3 = pd.read_sql(sql4, engine)
#print (df3)

ext_file_location = ("/home/ec2-user/Project/Audience_Bridge/weekly_data_info.csv")
df2.to_csv(ext_file_location, encoding='utf-8',  index = False, sep=',')    

send_mail(df,df1,df2,df3)
