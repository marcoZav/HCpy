#
# PER MONITORARE IL SISTEMA SERVE IL RUOLO PER VEDERE I JOB LANCIATI DAGLI ALTRI
#

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
   filename=logfolder+'\\'+'checkJobsStatus.log'
   , encoding='utf-8'
   , format='%(asctime)s %(levelname)-8s %(message)s'
   , datefmt='%Y-%m-%d %H:%M:%S'
   #, level=logging.DEBUG
   , level=logging.INFO
   )




'''
logger.debug('This message should go to the log file')
logger.info('So should this')
logger.warning('And this, too')
logger.error('And non-ASCII stuff, too, like Øresund and Malmö')
'''


batchJobsNumber2test=1
numberOfMinutesDelay4error=3
nDaysPastJobs=1
baseUrl = 'https://snamtest.ondemand.sas.com'

# dalle 12:00 ora italiana del 9 maggio, portati a 4 i job su mp (01--04)
# un errore il 18 maggio alle 13Z, proprio quando si è risolto il prob di azcopy
# sembra che i lmio job di login infinite abbia mandato il launcher in out of resources
# 3 jobs schedulati sono falliti e i pod sono tutti cmabiati, come una caduta e riavvio del launcher forse 
numberOfMinutesDelay4error=0
batchJobsNumber2test=4
nDaysPastJobs=1
baseUrl = 'https://snamprodmp.ondemand.sas.com'



batchJobsNumber2test=1
nDaysPastJobs=3
numberOfMinutesDelay4error=43
baseUrl = 'https://snamprodukjob.ondemand.sas.com'

numberOfMinutesDelay4error=0
batchJobsNumber2test=5
nDaysPastJobs=21
baseUrl = 'https://snamprodgerjob.ondemand.sas.com'




# -----------------------------------------------------------------------------------------------------------------





print('START ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------------------------------------------------------------------------')
print (baseUrl)
logger.info('START ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------------------------------------------------------------------------')
logger.info(baseUrl)

print('\n')
print('-- Get Token:')
logger.info('-- Get Token')

'''
out={"token": token, 
     "elapsedMs": elapsed,
     "httpStatusCode": responseStatusCode,
     "Response": responseText,
     "Description": outDesc,
     "Traceback": traceBackText
     }
'''
        
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
   print('*** EXITING ***')
   
   logger.error("httpStatusCode: " + str(httpStatusCode))
   logger.error('Description: ' + Description)
   if (Description=='GENERIC_ERROR'):
      logger.error('Traceback: '+traceBackText)
   logger.critical('**** errore durante la generazione del token: EXITING ****')
   sys.exit(1)

#print(token)
print('TOKEN ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------')





print('\n')
print('-- GET Jobs:')

maxIter=2
iter=1
endWhile=False


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
   jobsdata=[]
   for item in items:
    #print('================================================================================')
    #print(item)
    jobId=item['id']
    jobcreationTimeStamp=item['creationTimeStamp']
    jobmodifiedTimeStamp=item['modifiedTimeStamp']
    jobCreatedby=item['createdBy']
    jobmodifiedBy=item['modifiedBy']
    jobState=item['state']
    jobendTimeStamp=item['endTimeStamp'] if ('endTimeStamp' in item) else 'Missing'
    jobheartbeatTimeStamp=item['heartbeatTimeStamp'] if ('heartbeatTimeStamp' in item) else 'Missing'
    jobexpirationTimeStamp=item['expirationTimeStamp'] if ('expirationTimeStamp' in item) else 'Missing'
    jobsubmittedByApplication=item['submittedByApplication']
    jobheartbeatInterval=item['heartbeatInterval'] if ('heartbeatInterval' in item) else 'Missing'
    jobelapsedTime=item['elapsedTime']

    jobRequest=item['jobRequest']
    jobName=jobRequest['name']
    jobexpiresAfter=jobRequest['expiresAfter'] if ('expiresAfter' in item) else 'Missing'

    """
    print(jobId)
    print(jobState)
    print(jobcreationTimeStamp)
    
    print(jobmodifiedTimeStamp)
    print(jobCreatedby)
    print(jobendTimeStamp)
    print(jobsubmittedByApplication)
    print(jobexpiresAfter)
    """

    giorno =jobcreationTimeStamp[0:10];
    oraZ   =jobcreationTimeStamp[11:13];
    minuto =int(jobcreationTimeStamp[14:16]);
    
    #print(jobsubmittedByApplication)
    if (
       # job execution api 
       #(  jobsubmittedByApplication == 'SASJobExecution' ) 
       # job schedulati
       #| 
       (  jobsubmittedByApplication == 'jobExecution' ) 
       & ( jobName.find('jmon') == -1 )
       #& ( jobName.find('jmon') >= 0 )
       # passo tutto
       #| (True )
       ):
        jobsdata.append(
          {
              'jobId': jobId,
              'jobName': jobName,

              'giorno': giorno,
              'oraZ': oraZ,
              'minuto': minuto,

              'jobState': jobState,
              'jobCreatedby': jobCreatedby,
              'jobendTimeStamp': jobendTimeStamp,
              'jobsubmittedByApplication': jobsubmittedByApplication,
              'jobexpiresAfter': jobexpiresAfter,
              'jobelapsedTime': jobelapsedTime,
              #'jobheartbeatTimeStamp': jobheartbeatTimeStamp,
              #'jobheartbeatInterval': jobheartbeatInterval,
              'jobcreationTimeStamp':  jobcreationTimeStamp
          })
     
   #check se ci sono job del tipo filtrato sopra
   n=len(jobsdata) 
   if n==0:
      print ('Nessun Job presente del tipo selezionato in jobsubmittedByApplication')
   else:  
      dfJobs=pandas.DataFrame(jobsdata)
      pandas.set_option('display.max_rows', None)
      #pandas.set_option('display.max_columns', None)
      pandas.set_option('display.max_colwidth', None)
      print('\n')
      dfJobs.sort_values(by=['giorno','oraZ','jobName'], ascending=True)
      print(dfJobs)
      dfJobsSummary=dfJobs.groupby(['giorno','oraZ'], as_index = False).agg(
         {
         'minuto': ['min', 'max'], 
         'jobId': 'count',
         'jobName': ' '.join,
         'jobState': ' '.join
         })
      print('\n')
      print('dfJobsSummary')
      print(dfJobsSummary)

      """
      for col in dfJobsSummary.columns:
       print(col)
      """



      dfJobsAlert=dfJobsSummary.loc[
         ( dfJobsSummary[('jobId', 'count')] != batchJobsNumber2test )
         |  ( dfJobsSummary[('minuto', 'max')] > numberOfMinutesDelay4error) 
         |  ( dfJobsSummary[('jobState', 'join')].str.contains('failed') ) 
         ]
      print('\n')
      print('********************************************************************')
      #print(dfJobsAlert)
      if len(dfJobsAlert.index) == 0:
         print ('PASS')
      else:
         print ('FAIL')
         print ('\n')
         print ('Details:')

         dfJobsAlertNumberMore=dfJobsSummary.loc[
            ( dfJobsSummary[('jobId', 'count')] > batchJobsNumber2test )
            ]
         print('\n')
         print('Alert Numero Jobs partiti MAGGIORE del previsto: ')
         if dfJobsAlertNumberMore.empty:
            print ('OK')
         else:
            print(dfJobsAlertNumberMore)   


         dfJobsAlertStartTime=dfJobsSummary.loc[
            ( dfJobsSummary[('minuto', 'max')] > numberOfMinutesDelay4error) 
            ]
         print('\n')
         print('Alert Numero Jobs partiti in ritardo: ')
         if dfJobsAlertStartTime.empty:
            print ('OK')
         else:
            print(dfJobsAlertStartTime)


         dfJobsAlertNumberLess=dfJobsSummary.loc[
            ( dfJobsSummary[('jobId', 'count')] < batchJobsNumber2test )
            ]
         print('\n')
         print('Alert Numero Jobs partiti MINORE del previsto: ')
         if dfJobsAlertNumberLess.empty:
            print ('OK')
         else:
            print(dfJobsAlertNumberLess)





         dfJobsAlertFailed=dfJobsSummary.loc[
            ( dfJobsSummary[('jobState', 'join')].str.contains('failed') ) 
            ]
         print('\n')
         print('Alert Jobs FALLITI: ')
         if dfJobsAlertFailed.empty:
            print ('OK')
         else:
           print(dfJobsAlertFailed)



print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





