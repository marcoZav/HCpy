import subprocess

from datetime import datetime
from datetime import date, timedelta

#per prendere datetime.UTC
import datetime as dt    


from time import sleep


maxIter=3
iter=1

# UTC/GMT time
dt_utcnow = datetime.now(dt.UTC)
print('UTC time:',dt_utcnow)
print(dt_utcnow.hour, dt_utcnow.minute)

if ( dt_utcnow.minute > 0 ):
      #se siamo al minuto 3, devo sleeppare per 58 per partire al minuto 1
      s=60 - dt_utcnow.minute + 1
      print('slepping '+str(s)+' minutes...')
      sleep(s*60)

print ('start loop')
while ( iter <= maxIter ):
      print('################################################################')
      print('################################################################')
      dt_utcnow = datetime.now(dt.UTC)
      print('UTC time:',dt_utcnow)
      print(dt_utcnow.hour, dt_utcnow.minute)
      print('# ', iter , ' of ', maxIter)

      subprocess.call("c:/myPrograms/python-3.11.9-embed-amd64/python.exe c:/myProjects/HCpy/src/runMeasures.py 1")

      dt_utcnow = datetime.now(dt.UTC)
      s=60 - dt_utcnow.minute + 1
      iter=iter+1
      print('slepping '+str(s)+' minutes...')
      sleep(s*60)
      

