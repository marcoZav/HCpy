
from datetime import datetime
from datetime import date, timedelta

#per prendere datetime.UTC
import datetime as dt   

dateStart = date.today() - timedelta(hours=24)
print('Current Date :', date.today())
print(' dateStart :', dateStart) 

url =  "/jobExecution/jobs?start=0&limit=4000&sortBy=creationTimeStamp:ascending&filter=and(ge(creationTimeStamp,%27" + dateStart.strftime("%Y-%m-%d") + "T00:00:00.000Z%27))"
print (url)



dtNow=datetime.now(dt.UTC)
print(dtNow)
dtStart = dtNow - timedelta(hours=24)
print(dtStart)

print(      )

url =  "/jobExecution/jobs?start=0&limit=4000&sortBy=creationTimeStamp:ascending&filter=and(ge(creationTimeStamp,%27" + dtStart.strftime("%Y-%m-%dT%H:%M:%S.000Z") + "%27))"
print (url)