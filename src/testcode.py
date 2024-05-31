
from datetime import datetime
from datetime import date, timedelta
import traceback 

dateStart = date.today() - timedelta(hours=24)
print('Current Date :', date.today())
print(' dateStart :', dateStart) 

url = baseUrl + "/jobExecution/jobs?start=0&limit=4000&sortBy=creationTimeStamp:ascending&filter=and(ge(creationTimeStamp,%27" + dateStart.strftime("%Y-%m-%d") + "T00:00:00.000Z%27))"
print (url)