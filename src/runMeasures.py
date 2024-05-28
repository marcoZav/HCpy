

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

#per prendere datetime.UTC
import datetime as dt    

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
      print('-- RUN Job Executions')
      pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getComputePodsNumber.sas'

      response=sasapi.runJobExecution(baseUrl,token,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url',"pgm_url=" + pgmUrl)
      responseText=response.text
      #print(responseText)
      rj = json.loads(responseText)
      numPods=rj['NumPods']
      print(numPods)

      m=sasapi.Measure(baseUrl,datetime.now(),'NUM_COMPUTE_PODS',str(numPods),'')
      stats.handleMeasure(m)


      pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getSnmFileSystemPctUsed.sas'

      response=sasapi.runJobExecution(baseUrl,token,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url',"pgm_url=" + pgmUrl)
      responseText=response.text
      #print(responseText)
      rj = json.loads(responseText)
      snmPctUsed=rj['snm_pctUsed']
      print(snmPctUsed)

      m=sasapi.Measure(baseUrl,datetime.now(),'SNM_PCT_USED',str(snmPctUsed),'')
      stats.handleMeasure(m)


      print('\n')
      print('-- GET Jobs')

      maxIter=2
      iter=1
      endWhile=False

      nDaysPastJobs=0

      if (   baseUrl == 'https://snamprodgerjob.ondemand.sas.com' 
           or baseUrl == 'https://snamprodukjob.ondemand.sas.com'
           or baseUrl == 'https://snamprodmp.ondemand.sas.com'
           ):
         
         if baseUrl == 'https://snamprodgerjob.ondemand.sas.com':
            numberOfMinutesDelay4error=0
            batchJobsNumber2test=6
         if baseUrl == 'https://snamprodukjob.ondemand.sas.com':
            numberOfMinutesDelay4error=43
            batchJobsNumber2test=1
         if baseUrl == 'https://snamprodmp.ondemand.sas.com':
            numberOfMinutesDelay4error=0
            batchJobsNumber2test=4

         while ( ( iter <= maxIter ) & ( endWhile == False ) ):
            # terzo parametro, numero di giorni indietro
            items=sasapi.getJobs(baseUrl,token,nDaysPastJobs)
            #print(items)
            if ( len(items) == 0 ):
               print('ELENCO JOBS RESTITUITO HA ZERO ELEMENTI. Iter=', iter)
               iter=iter+1
               sleep(5)
            else:
               endWhile=True
         
            if ( len(items) == 0 ):
               print('ELENCO JOBS RESTITUITO HA ZERO ELEMENTI')
            else:
               jobsdata=sasapi.buildJobsDataTable(items)     
            
         #check se ci sono job del tipo filtrato sopra
         n=len(jobsdata) 
         if n==0:
            print ('Nessun Job presente del tipo selezionato in jobsubmittedByApplication')
            # todo - da gestire se info, critical ecc.
            stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_JOBS_NOTFOUND','0','Nessun Job presente del tipo selezionato in jobsubmittedByApplication'))
         else:  
            dfJobs=pandas.DataFrame(jobsdata)
            pandas.set_option('display.max_rows', None)
            #pandas.set_option('display.max_columns', None)
            pandas.set_option('display.max_colwidth', None)
            print('\n')
            dfJobs.sort_values(by=['giorno','oraZ','jobName'], ascending=True)
            #print(dfJobs)
            
            # UTC/GMT time
            dt_utcnow = datetime.now(dt.UTC)
            print('UTC time:',dt_utcnow)
            print(dt_utcnow.hour, dt_utcnow.minute)
            
            dfJobsCurrentHour=dfJobs.loc[( dfJobs['oraZ'] == str(dt_utcnow.hour) ) ]
            print('Dettaglio Jobs di questa ORA:')
            print(dfJobsCurrentHour)

            dfJobsSummary=dfJobs.groupby(['giorno','oraZ'], as_index = False).agg(
               {
                  'minuto': ['min', 'max'], 
                  'jobId': 'count',
                  'jobName': ' '.join,
                  'jobState': ' '.join
                  })
            print('\n')
            
            dfJobsSummaryCurrentHour=dfJobsSummary.loc[
               ( dfJobsSummary['oraZ'] == str(dt_utcnow.hour) ) 
               ]
            print('Summary Jobs di questa ORA:')
            print(dfJobsSummaryCurrentHour)
            
            dfJobsAlert=dfJobsSummaryCurrentHour.loc[
               ( dfJobsSummaryCurrentHour[('jobId', 'count')] < batchJobsNumber2test )
               |  ( dfJobsSummaryCurrentHour[('minuto', 'max')] > numberOfMinutesDelay4error) 
               |  ( dfJobsSummaryCurrentHour[('jobState', 'join')].str.contains('failed') ) 
               ]
            print('\n')
            print(dfJobsAlert)
            if len(dfJobsAlert.index) == 0:
               stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dt_utcnow.hour),'OK'))
               print ('PASS')
            else:
               print ('FAIL')
               print ('\n')
               print ('Details:')
               
               dfJobsAlertNumberMore=dfJobsSummaryCurrentHour.loc[
                  ( dfJobsSummaryCurrentHour[('jobId', 'count')] > batchJobsNumber2test )
                  ]
               print('\n')
               print('Alert Numero Jobs partiti MAGGIORE del previsto: ')
               if dfJobsAlertNumberMore.empty:
                  print ('OK')
               else:
                  print(dfJobsAlertNumberMore)   
                  stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dfJobsAlertNumberMore.count),'Jobs partiti MAGGIORE del previsto'))
               
               dfJobsAlertStartTime=dfJobsSummaryCurrentHour.loc[( dfJobsSummaryCurrentHour[('minuto', 'max')] > numberOfMinutesDelay4error) ]
               print('\n')
               print('Alert Numero Jobs partiti in ritardo: ')
               if dfJobsAlertStartTime.empty:
                  print ('OK')
               else:
                  print(dfJobsAlertStartTime)
                  stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dfJobsAlertStartTime.count),'Jobs partiti in ritardo'))

               dfJobsAlertNumberLess=dfJobsSummaryCurrentHour.loc[( dfJobsSummaryCurrentHour[('jobId', 'count')] < batchJobsNumber2test ) ]
               print('\n')
               print('Alert Numero Jobs partiti MINORE del previsto: ')
               if dfJobsAlertNumberLess.empty:
                  print ('OK')
               else:
                  print(dfJobsAlertNumberLess)
                  stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dfJobsAlertNumberLess.count),'Jobs partiti MINORE del previsto'))

               dfJobsAlertFailed=dfJobsSummaryCurrentHour.loc[ ( dfJobsSummaryCurrentHour[('jobState', 'join')].str.contains('failed') )  ]
               print('\n')
               print('Alert Jobs FALLITI: ')
               if dfJobsAlertFailed.empty:
                  print ('OK')
               else:
                  print(dfJobsAlertFailed)
                  stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dfJobsAlertFailed.count),'Jobs FALLITI'))


print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')



      
print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





