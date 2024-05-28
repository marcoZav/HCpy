#
# PER MONITORARE IL SISTEMA SERVE IL RUOLO PER VEDERE I JOB LANCIATI DAGLI ALTRI
#
# cpc


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

import sasapi



import pandas
from datetime import datetime

from time import sleep






# dalle 12:00 ora italiana del 9 maggio, portati a 4 i job su mp (01--04)
# un errore il 18 maggio alle 13Z, proprio quando si è risolto il prob di azcopy
# sembra che i lmio job di login infinite abbia mandato il launcher in out of resources
# 3 jobs schedulati sono falliti e i pod sono tutti cmabiati, come una caduta e riavvio del launcher forse 
numberOfMinutesDelay4error=0
batchJobsNumber2test=4
nDaysPastJobs=1
baseUrl = 'https://snamprodmp.ondemand.sas.com'




numberOfMinutesDelay4error=0
batchJobsNumber2test=5
nDaysPastJobs=7
baseUrl = 'https://snamprodgerjob.ondemand.sas.com'



batchJobsNumber2test=1
numberOfMinutesDelay4error=3
nDaysPastJobs=1
baseUrl = 'https://snamtest.ondemand.sas.com'


batchJobsNumber2test=1
nDaysPastJobs=3
numberOfMinutesDelay4error=43
baseUrl = 'https://snamprodukjob.ondemand.sas.com'

# -----------------------------------------------------------------------------------------------------------------


print('START ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------------------------------------------------------------------------')
print (baseUrl)

print('\n')
print('-- Get Token:')

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
   sys.exit(1)

#print(token)
print('TOKEN ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------')


print('\n')
print('-- Get sas.scheduler Conf:')
items=sasapi.getConfigurationDefinition(baseUrl,token,'sas.scheduler')
print(items)

print('\n')
print('-- Get sas.authorization Conf:')
items=sasapi.getConfigurationDefinition(baseUrl,token,'sas.authorization')
print(items)


print('\n')
print('-- Get Identities Conf:')
items=sasapi.getConfigurationDefinition(baseUrl,token,'sas.identities')
#items=getIdentitiesConf(baseUrl,token)
print(items)

'''
alcune altre forse interessanti:
sas.authorization
oauth2.client      durata token anche qui
sas.logon.callback
sas.logon.custom
sas.logon.groups
sas.logon.initial
sas.logon.jwt       questa per la durata dei token
sas.logon.oauth.providers
sas.logon.oauth.providers.external_oauth
sas.logon.sessions
'''


print('\n')
print('-- Get Compute Contexts:')

items=sasapi.getComputeContexts(baseUrl,token)
for item in items:
  contextName=item['name'] 
  contextId=item['id'] 
  if ( contextName == 'SAS Job Execution compute context' ):  jobExecutionContextId=contextId
  print(contextName + ': id=' + contextId)

print('\n')
print ('jobExecutionContextId='+jobExecutionContextId);






print('\n')
print('-- Get Job Execution Compute Context details:')


contextAttributes=sasapi.getContextDefinitionAttributes(baseUrl,jobExecutionContextId,token)
print(' context Attributes:')
print (contextAttributes)



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





print('\n')
print('-- RUN Job Execution:')

pgmUrl = 'https://raw.githubusercontent.com/marcoZav/test01/master/Program1.sas'
pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getComputePods.sas'
#pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getPodByName.sas&parms=sas-compute-server-0accbd2e-1485-46c9-99c4-1c8e883e5614-35387';

response=sasapi.runJobExecution(baseUrl,token,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url',"pgm_url=" + pgmUrl)
print(response.text)

print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





