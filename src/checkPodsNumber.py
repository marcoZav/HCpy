

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
   filename=logfolder+'\\'+'checkPodslog'
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

for baseUrl in baseUrls:
   print (baseUrl)
   logger.info(baseUrl)
   print('-- Get Token:')
   logger.info('-- Get Token')
       
   maxIter=2
   iter=1
   endWhile=False

   while ( ( iter <= maxIter ) & ( endWhile == False ) ):
      print('Tentative # ', iter , ' of ', maxIter)
      out=sasapi.getToken(baseUrl)
   
      token=out["token"]
      elapsed=out["elapsedMs"]
      httpStatusCode=out["httpStatusCode"]
      Description=out["Description"]
      traceBackText=out['Traceback']
   
      print("httpStatusCode", httpStatusCode)
      print('Ms: ', elapsed)
   
      if ( httpStatusCode != 200):
         print('Description',Description)
         if (Description=='GENERIC_ERROR'):
            print('Traceback',traceBackText)
         iter=iter+1
         sleep(5)
      else:
         endWhile=True

   if ( httpStatusCode != 200 ):
      print('*** SKIP ***')
   
      logger.error("httpStatusCode: " + str(httpStatusCode))
      logger.error('Description: ' + Description)
      if (Description=='GENERIC_ERROR'):
         logger.error('Traceback: '+traceBackText)
      logger.critical('**** errore durante la generazione del token: SKIP parte successiva ****')
      #sys.exit(1)
   else:
      print('TOKEN ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------')
      print('-- RUN Job Execution:')
      pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getComputePods.sas'

      response=sasapi.runJobExecution(baseUrl,token,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url',"pgm_url=" + pgmUrl)
      print(response.text)

print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





