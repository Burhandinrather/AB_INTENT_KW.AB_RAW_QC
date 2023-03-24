import os
import smtplib
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
# ESTABLISHING CONNECTION WITH THE SNOWFLAKE WAREHOUSE 
engine = create_engine(URL(
   account='nua76068.us-east-1',
   user='BURHAND',
   password='Core@123',
   database='AB_INTENT_KW',
   schema='INBOUND',
   warehouse='COMPUTE_WH',
   role='SYSADMIN',
))


#send mail
def send_mail(df,df1,df2):
    msg = MIMEMultipart()
    msg['Subject'] = 'Audience Bridge QC'
    msg['From'] = 'ratherburhan101@gmail.com'
    msg['To'] = ("burhan.din@transmissionagency.com")

    # Set the email body text to include the dataframe information
    if len(df) > 0:
        body_text1 = f' Hi Team, \n\n The Data has been loaded successfully on {df["updated_at"][0]}.\n The total Number of Rows loaded = {df["rows_loaded"][0]}\n Date Range: {df["min_date"][0]} to {df["max_date"][0]}\n\n'
        #msg.attach(MIMEText(body_text1))
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

    body_text = f'{body_text1}\n{body_text2}\n{body_text3}\n\nRegards,\nBurhan ud din'

    msg.attach(MIMEText(body_text))
            

    with smtplib.SMTP('smtp.gmail.com',587) as smtp:
        smtp.starttls()
        smtp.login('ratherburhan101@gmail.com', 'saxpbdmmeycvgbtx')
        smtp.send_message(msg)
        print('Mail sent')

 
# FETCHING THE DATA FROM SNOWFLAKE USING SQL AND STORING IT IN A VARIABLE

sql1='''
select * from AB_INTENT_KW.INBOUND.AB_RAW_INFO;
'''

sql2='''
select COUNTRY from AB_INTENT_KW.INBOUND.COUNTRY WHERE COUNTRY NOT IN (
select COUNTRYCODE From AB_INTENT_KW.INBOUND.AB_RAW where COUNTRYCODE IS NOT NULL 
AND date(timestamp) BETWEEN (select MIN_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1)
AND  (select MAX_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1));
''' 

sql3 = '''
select KEYWORD from "AB_INTENT_KW"."OUTBOUND"."TOPICS_MAPPING" where CLIENT = 'Maersk' AND KEYWORD NOT IN 
(select distinct replace(KWTEXT,'_',' ') AS KEYWORDTEXT from AB_INTENT_KW.INBOUND.AB_RAW
where date(timestamp) BETWEEN (select MIN_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1)
AND  (select MAX_DATE from AB_INTENT_KW.INBOUND.AB_RAW_INFO ORDER BY ID DESC LIMIT 1))
'''

# CONVERTING THE FETCHED DATA INTO PANDAS DATAFRAME FOR EASY MANIPULATIONS AND WRITING TO FILE
df = pd.read_sql(sql1, engine)
#print(df)
df1 = pd.read_sql(sql2, engine)
#print (df1)
df2 = pd.read_sql(sql3, engine)
#print (df2)

ext_file_location = (r"C:\Work\Audience Bridge\AB_RAW_QC\missing_keywords.csv")
df2.to_csv(ext_file_location, encoding='utf-8',  index = False, sep=',')    

send_mail(df,df1,df2)