

import os
import sys

# aggiungo al path quello corrente di questo programma
# e una cartella allo stesso livello che si chiama modules
currentPyFileAbspath = os.path.abspath(__file__)
dname = os.path.dirname(currentPyFileAbspath)
os.chdir(dname)
#print(dname)
sys.path.insert(0, dname)

# uso / al posto di \\ per poter girare anche su linux
#sys.path.insert(0, dname + '\\..\\modules')
sys.path.insert(0, dname + '/../modules')

#logfolder=dname + '\\..\\logs'
logfolder=dname + '/../logs'
print(logfolder)

cfgFile=dname + '/../cfg/cfg.ini'
print(cfgFile)


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
   #per funzionare anche su linux
   #filename=logfolder+'\\'+'stats.log'
   filename=logfolder+'/'+'stats.log'
   , encoding='utf-8'
   , format='%(asctime)s %(levelname)-8s %(message)s'
   , datefmt='%Y-%m-%d %H:%M:%S'
   #, level=logging.DEBUG
   , level=logging.INFO
   )


restApi=sasapi.RestApi(logger,cfgFile)


baseUrls = [
   'https://snamtest.ondemand.sas.com'
  ,'https://snamprodmp.ondemand.sas.com'
  ,'https://snamprodukjob.ondemand.sas.com'
  ,'https://snamprodgerjob.ondemand.sas.com'
  ]

# debug
#baseUrls = ['https://snamprodgerjob.ondemand.sas.com' ]

# -----------------------------------------------------------------------------------------------------------------

print('START ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------------------------------------------------------------------------')

stats=sasapi.Stats(logger)


for baseUrl in baseUrls:
   print (baseUrl)
   print('-- Get Token:')
       
   maxIter=5
   iter=1
   endWhile=False

   while ( ( iter <= maxIter ) & ( endWhile == False ) ):
      print('Tentative # ', iter , ' of ', maxIter)
      out=restApi.getToken(baseUrl)
   
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
         
         stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_TOKEN_RETRY',str(httpStatusCode),description))
         sleep(5*iter)
         iter=iter+1
      else:
         endWhile=True

   if ( httpStatusCode != 200 ):
      print('*** SKIP ***')
   
      if (description=='GENERIC_ERROR'):
         description=traceBackText

      stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_TOKEN_ERROR',str(httpStatusCode),description))
      #sys.exit(1)
   else:
      stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_TOKEN_ELAPSED',str(elapsed),''))

      print('TOKEN ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------')
      print('-- RUN Job Executions')

      print('\n -- NUM_COMPUTE_PODS')
      pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getComputePodsNumber.sas'

      out=restApi.runJobExecution(baseUrl,token,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url',"pgm_url=" + pgmUrl)

      elapsed=out["elapsedMs"]
      httpStatusCode=out["httpStatusCode"]
      Description=out["Description"]
      traceBackText=out['Traceback']
      response=out["Response"]
   
      print("httpStatusCode", httpStatusCode)
      print('Ms: ', elapsed)
      print('*** runJobExecution response: *** \n '+response)

      if ( httpStatusCode != 200):
         print('Description',Description)
         if (Description=='GENERIC_ERROR'):
            print('Traceback',traceBackText)
         stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_NUM_COMPUTE_PODS_ERROR',str(httpStatusCode),response+'///'+Description+'///'+traceBackText))

      else:
         rj = json.loads(response)
         numPods=rj['NumPods']
         print('num pods:',numPods)

         m=sasapi.Measure(baseUrl,datetime.now(),'NUM_COMPUTE_PODS',str(numPods),'')
         stats.handleMeasure(m)


      print('\n -- SNM_PCT_USED')
      pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getSnmFileSystemPctUsed.sas'

      out=restApi.runJobExecution(baseUrl,token,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url',"pgm_url=" + pgmUrl)

      elapsed=out["elapsedMs"]
      httpStatusCode=out["httpStatusCode"]
      Description=out["Description"]
      traceBackText=out['Traceback']
      response=out["Response"]
   
      print("httpStatusCode", httpStatusCode)
      print('Ms: ', elapsed)
      print('*** runJobExecution response: *** \n '+response)

      if ( httpStatusCode != 200):
         print('Description',Description)
         if (Description=='GENERIC_ERROR'):
            print('Traceback',traceBackText)
         stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_SNM_PCT_USED_ERROR',str(httpStatusCode),response+'///'+Description+'///'+traceBackText))

      else:
         rj = json.loads(response)
         snmPctUsed=rj['snm_pctUsed']
         print('file system /snm pctused:',snmPctUsed)
         stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'SNM_PCT_USED',str(snmPctUsed),'') )


      

     


      print('\n')
      print('-- GET Jobs')

      maxIter=2
      iter=1
      endWhile=False

      nHoursPastJobs=1

      if (   baseUrl == 'https://snamprodgerjob.ondemand.sas.com' 
            or baseUrl == 'https://snamprodukjob.ondemand.sas.com'
            or baseUrl == 'https://snamprodmp.ondemand.sas.com'
            or baseUrl == 'https://snamtest.ondemand.sas.com'
           ):
         
         if baseUrl == 'https://snamprodgerjob.ondemand.sas.com':
            numberOfMinutesDelay4error=0
            batchJobsNumber2test=0
         if baseUrl == 'https://snamprodukjob.ondemand.sas.com':
            numberOfMinutesDelay4error=43
            batchJobsNumber2test=0
         if baseUrl == 'https://snamprodmp.ondemand.sas.com':
            numberOfMinutesDelay4error=0
            batchJobsNumber2test=2
         if baseUrl == 'https://snamtest.ondemand.sas.com':
            numberOfMinutesDelay4error=0
            batchJobsNumber2test=1

         while ( ( iter <= maxIter ) & ( endWhile == False ) ):
            # terzo parametro, numero di ore indietro
            items=restApi.getJobs(baseUrl,token,nHoursPastJobs)
            #print(items)
            if ( len(items) == 0 ):
               print('ELENCO JOBS RESTITUITO HA ZERO ELEMENTI. Iter=', iter)
               iter=iter+1
               sleep(5)
            else:
               endWhile=True
         
            if ( len(items) == 0 ):
               print('ELENCO JOBS RESTITUITO HA ZERO ELEMENTI')
               stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'GET_JOBS_EMPTY','0','ELENCO JOBS RESTITUITO HA ZERO ELEMENTI'))
            else:
               jobsdata=restApi.buildJobsDataTable(items)     
               
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
                  print('dfJobs - tutti quelli estratti per data e tipologia')
                  print(dfJobs)
                  
                  # UTC/GMT time
                  dt_utcnow = datetime.now(dt.UTC)
                  print('UTC time:',dt_utcnow)
                  print(dt_utcnow.date(),dt_utcnow.hour, dt_utcnow.minute)
                  
                  dfJobsCurrentHour=dfJobs.loc[ ( dfJobs['oraZ'] == str(dt_utcnow.hour).zfill(2) )  & ( dfJobs['giorno'] == str(dt_utcnow.date()) ) ]
                  

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
                  
                  dfJobsSummaryCurrentHour=dfJobsSummary.loc[ ( dfJobsSummary['oraZ'] == str(dt_utcnow.hour).zfill(2) ) & ( dfJobs['giorno'] == str(dt_utcnow.date()) ) ]
                           
                  # debug
                  #dfJobsSummaryCurrentHour=dfJobsSummary.loc[ ( dfJobsSummary['oraZ'] == '99' ) ]


                  print('Summary Jobs di questa ORA:')
                  print(dfJobsSummaryCurrentHour)

                  if ( len(dfJobsSummaryCurrentHour.index) == 0 ):
                     print ('FAIL - nessuno partito')
                     # nessun Job partito
                     stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dt_utcnow.hour),'NESSUNO_PARTITO'))
                  else:
                     # questi controlli vanno se esiste almeno una riga
                     dfJobsAlertCurrentHour=dfJobsSummaryCurrentHour.loc[
                        ( dfJobsSummaryCurrentHour[('jobId', 'count')] < batchJobsNumber2test )
                        |  ( dfJobsSummaryCurrentHour[('minuto', 'max')] > numberOfMinutesDelay4error) 
                        |  ( dfJobsSummaryCurrentHour[('jobState', 'join')].str.contains('failed') ) 
                        ]
                     print('\n')
                     print('dfJobsAlertCurrentHour:')
                     print(dfJobsAlertCurrentHour)
                     if len(dfJobsAlertCurrentHour.index) == 0:
                        stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dt_utcnow.hour),'OK'))
                        print ('PASS')
                     else:
                        print ('FAIL')
                        print ('\n')
                        print ('Details:')
                        
                        dfJobsAlertCurrentHourNumberMore=dfJobsSummaryCurrentHour.loc[
                           ( dfJobsSummaryCurrentHour[('jobId', 'count')] > batchJobsNumber2test )
                           ]
                        print('\n')
                        print('Alert Numero Jobs partiti MAGGIORE del previsto ('+str(batchJobsNumber2test)+'): ')
                        if dfJobsAlertCurrentHourNumberMore.empty:
                           print ('OK')
                        else:
                           print(dfJobsAlertCurrentHourNumberMore)   
                           stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dfJobsAlertCurrentHourNumberMore[('jobId', 'count')].values[0]),'Jobs partiti MAGGIORE del previsto ('+str(batchJobsNumber2test)+')'))
                        
                        dfJobsAlertCurrentHourStartTime=dfJobsSummaryCurrentHour.loc[( dfJobsSummaryCurrentHour[('minuto', 'max')] > numberOfMinutesDelay4error) ]
                        print('\n')
                        print('Alert Numero Jobs partiti in ritardo (atteso delay massimo al minuto '+str(numberOfMinutesDelay4error)+'): ')
                        if dfJobsAlertCurrentHourStartTime.empty:
                           print ('OK')
                        else:
                           print(dfJobsAlertCurrentHourStartTime)
                           stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dfJobsAlertCurrentHourStartTime[('jobId', 'count')].values[0]),'Jobs partiti in ritardo (atteso delay massimo al minuto '+str(numberOfMinutesDelay4error)+')'))

                        dfJobsAlertCurrentHourNumberLess=dfJobsSummaryCurrentHour.loc[( dfJobsSummaryCurrentHour[('jobId', 'count')] < batchJobsNumber2test ) ]
                        print('\n')
                        print('Alert Numero Jobs partiti MINORE del previsto ('+str(batchJobsNumber2test)+'): ')
                        if dfJobsAlertCurrentHourNumberLess.empty:
                           print ('OK')
                        else:
                           print(dfJobsAlertCurrentHourNumberLess)
                           stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(  dfJobsAlertCurrentHourNumberLess[('jobId', 'count')].values[0]  ),'Jobs partiti MINORE del previsto ('+str(batchJobsNumber2test)+')'))

                        dfJobsAlertCurrentHourFailed=dfJobsSummaryCurrentHour.loc[ ( dfJobsSummaryCurrentHour[('jobState', 'join')].str.contains('failed') )  ]
                        print('\n')
                        print('Alert Jobs FALLITI: ')
                        if dfJobsAlertCurrentHourFailed.empty:
                           print ('OK')
                        else:
                           print(dfJobsAlertCurrentHourFailed)
                           stats.handleMeasure(sasapi.Measure(baseUrl,datetime.now(),'CHECK_JOBS_HOURLY',str(dfJobsAlertCurrentHourFailed[('jobId', 'count')].values[0]),'Jobs FALLITI'))


print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





