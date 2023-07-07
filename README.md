# AB_RAW_QC
This Python Script is mainly used to QC the AB_RAW data that's getting loaded each week using the Snow Pipe into the AB_INTENT_KW.INBOUND.AB_RAW table.
This Script will notify us below the following points via email:
The script is using the Master Table AB_INTENT_KW.AB_RAW in which the data is directly getting appended from the S3 using the snowpipe.
Also, I have created the streams and tasks in Snowflake using SQL to capture some mandatory info regarding the data that gets appended every week. The location to the scrip is here: https://github.com/Burhandinrather/AB_INTENT_KW.AB_RAW_QC/blob/master/Stream_and_task.txt
It will notify us regarding the date range, number of rows, and importing date of the data that we receive each week.
It will notify us whether all the 15 countries are present in the data or not. If any of the countries are not listed in the data. It will provide us a list of those countries as well
It will notify us regarding the matching and non-matching percentage of Maersk Keywords present in the data. If the matching keyword percentage is below 90% then It will send us an attachment of non-matching keywords in an email.
If the matching keywords are >= 90% then, It will simply send us a text message in the email that we have got say(95%) matching Keywords.
The people who will receive the email are: Burhan ud din,Neel Kamal, and Claire Gardener.
