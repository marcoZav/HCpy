

import os
import sys

# aggiungo al path quello corrente di questo programma
# e una cartella allo stesso livello che si chiama modules
currentPyFileAbspath = os.path.abspath(__file__)
dname = os.path.dirname(currentPyFileAbspath)
os.chdir(dname)
#print(dname)
sys.path.insert(0, dname)
sys.path.insert(0, dname + '\\..\\modules')

logfolder=dname + '\\..\\logs'
print(logfolder)


import sasapi


import requests
import json
import pandas

from datetime import datetime
from datetime import date, timedelta
from time import sleep
import traceback 
import sys



import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
   filename=logfolder+'\\'+'stats.log'
   , encoding='utf-8'
   , format='%(asctime)s %(levelname)-8s %(message)s'
   , datefmt='%Y-%m-%d %H:%M:%S'
   #, level=logging.DEBUG
   , level=logging.INFO
   )




baseUrls = [
   'https://snamtest.ondemand.sas.com'
  ,'https://snamprodmp.ondemand.sas.com'
  ,'https://snamprodukjob.ondemand.sas.com'
  ,'https://snamprodgerjob.ondemand.sas.com'
  ]



# -----------------------------------------------------------------------------------------------------------------

print('START ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------------------------------------------------------------------------')

stats=sasapi.Stats(logger)


for baseUrl in baseUrls:
   print (baseUrl)
   print('-- Get Token:')
       
   maxIter=3
   iter=1
   endWhile=False

   while ( ( iter <= maxIter ) & ( endWhile == False ) ):
      print('Tentative # ', iter , ' of ', maxIter)
      out=sasapi.getToken(baseUrl)
   
      token=out["token"]
      elapsed=out["elapsedMs"]
      httpStatusCode=out["httpStatusCode"]
      description=out["Description"]
      traceBackText=out['Traceback']
   
      print("httpStatusCode", httpStatusCode)
      print('Ms: ', elapsed)
   
      if ( httpStatusCode != 200):
         print('description',description)
         if (description=='GENERIC_ERROR'):
            description=traceBackText
            print('Traceback',description)
         iter=iter+1
         stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_TOKEN_RETRY',httpStatusCode,description))
         sleep(5)
      else:
         endWhile=True

   if ( httpStatusCode != 200 ):
      print('*** SKIP ***')
   
      if (description=='GENERIC_ERROR'):
         description=traceBackText

      stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_TOKEN_ERROR',httpStatusCode,description))
      #sys.exit(1)
   else:
      stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_TOKEN_ELAPSED',str(elapsed),''))

      print('TOKEN ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------')
      print('-- RUN Job Execution:')
      pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getComputePodsNumber.sas'

      response=sasapi.runJobExecution(baseUrl,token,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url',"pgm_url=" + pgmUrl)
      responseText=response.text
      #print(responseText)
      rj = json.loads(responseText)
      numPods=rj['NumPods']
      print(numPods)

      m=sasapi.Measure(baseUrl,datetime.now(),'NUM_COMPUTE_PODS',str(numPods),'')
      stats.handleMeasure(m)
      
print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





