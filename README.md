# AB_RAW_QC
This Python Script is mainly used to QC the AB_RAW data that's getting loaded each week using the Snow Pipe into the AB_INTENT_KW.INBOUND.AB_RAW table.
This Script will notify us below the following points via email:
It will notify us regarding the date range, number of rows, and importing date of the data that we receive each week.
It will notify us whether all the 15 countries are present in the data or not. If any of the countries are not listed in the data. It will provide us a list of those countries as well
It will notify us regarding the matching and non-matching percentage of Maersk Keywords present in the data. If the matching keyword percentage is below 90% then It will send us an attachment of non-matching keywords in an email.
If the matching keywords are >= 90% then, It will simply send us a text message in the email that we have got say(95%) matching Keywords.
The people who will receive the email are: Burhan ud din,Neel Kamal, and Claire Gardener.
