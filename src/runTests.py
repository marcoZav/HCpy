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

import sasapi

import pandas

from datetime import datetime
from time import sleep

import os
import sys

currentPyFileAbspath = os.path.abspath(__file__)
dname = os.path.dirname(currentPyFileAbspath)
#logfolder=dname + '\\..\\logs'
logfolder=dname + '/../logs'
print(logfolder)

cfgFile=dname + '/../cfg/cfg.ini'
print(cfgFile)



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

contentPathJobExRunPgm=restApi.jobexec_pgm_from_url








# oltre ai non partiti, vedere i failed, da cui si vedono le http error, di cui pare esserci tuning
batchJobsNumber2test=1
numberOfMinutesDelay4error=3
nHoursPastJobs=200
baseUrl = 'https://snamtest.ondemand.sas.com'


batchJobsNumber2test=2
nHoursPastJobs=24*3
numberOfMinutesDelay4error=43
baseUrl = 'https://snamprodukjob.ondemand.sas.com'




numberOfMinutesDelay4error=0
batchJobsNumber2test=2
nHoursPastJobs=24*3
baseUrl = 'https://snamprodgerjob.ondemand.sas.com'   


numberOfMinutesDelay4error=15
batchJobsNumber2test=1
nHoursPastJobs=24*8
baseUrl = 'https://snamprodmp.ondemand.sas.com'



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
        
maxIter=4
iter=1
endWhile=False

while ( ( iter <= maxIter ) & ( endWhile == False ) ):
   print('Tentative # ', iter , ' of ', maxIter)
   out=restApi.getToken(baseUrl)
   
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
      
      sleep(5*iter)
      iter=iter+1
   else:
      endWhile=True

if ( httpStatusCode != 200 ):
   print('*** EXITING ***')
   sys.exit(1)

#print(token)
print('TOKEN ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------')





'''
print('\n')
print('sendmail')

# FORMATO DA PASSARE PER INDIRIZZI MULTIPLI 
ret=restApi.sendMail(baseUrl,token,'("marco.zavarini@sas.com" "marco.zavarini@sas.com")','test','test\nbody')

# \n va a capo correttamente
#ret=restApi.sendMail(baseUrl,token,'"marco.zavarini@sas.com"','test','testbody \n ciao3')

print(ret)
'''





print('\n')
print('-- Get sas.scheduler Conf:')
items=restApi.getConfigurationDefinition(baseUrl,token,'sas.scheduler')
print(items)

print('\n')
print('-- Get sas.authorization Conf:')
items=restApi.getConfigurationDefinition(baseUrl,token,'sas.authorization')
print(items)


print('\n')
print('-- Get Identities Conf:')
items=restApi.getConfigurationDefinition(baseUrl,token,'sas.identities')
#items=getIdentitiesConf(baseUrl,token)
print(items)


print('\n')
print('-- Get sas.logon.jwt Conf:')
items=restApi.getConfigurationDefinition(baseUrl,token,'sas.logon.jwt')
#items=getIdentitiesConf(baseUrl,token)
print(items)

print('\n')
print('-- Get oauth2.client Conf:')
items=restApi.getConfigurationDefinition(baseUrl,token,'oauth2.client')
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

items=restApi.getComputeContexts(baseUrl,token)
for item in items:
  contextName=item['name'] 
  contextId=item['id'] 
  if ( contextName == 'SAS Job Execution compute context' ):  jobExecutionContextId=contextId
  print(contextName + ': id=' + contextId)

print('\n')
print ('jobExecutionContextId='+jobExecutionContextId);






print('\n')
print('-- Get Job Execution Compute Context details:')


contextAttributes=restApi.getContextDefinitionAttributes(baseUrl,jobExecutionContextId,token)
print(' context Attributes:')
print (contextAttributes)



print('\n')
print('-- GET Jobs:')

maxIter=2
iter=1
endWhile=False


while ( ( iter <= maxIter ) & ( endWhile == False ) ):
   # terzo parametro, numero di ORE indietro
   items=restApi.getJobs(baseUrl,token,nHoursPastJobs)
   #print(items)
   if ( len(items) == 0 ):
      print('ELENCO JOBS RESTITUITO HA ZERO ELEMENTI. Iter=', iter)
      iter=iter+1
      sleep(5)
   else:
      endWhile=True


startGiorno=''
startOraZ=''
endGiorno=''
endOraZ=''
flgFirstRow=True

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

    if (flgFirstRow):
       startGiorno=giorno
       startOraZ=oraZ
       flgFirstRow=False
    endGiorno=giorno
    endOraZ=oraZ
    
    #print(jobsubmittedByApplication)
    if (
       # job execution api 
       #(  jobsubmittedByApplication == 'SASJobExecution' ) 
       # job schedulati
       #| 
       (  jobsubmittedByApplication == 'jobExecution' ) 
       # & ( jobName.find('jmon') == -1 )
       # dafne
       & ( jobName.find('g1_prevedi_viya_job') >= 0 )
       #| ( jobName.find('JD') >= 0 )
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

      print('\n CHECK ORE SENZA JOB PARTITI: \n')
      print('********************************************************************')

      print('start giorno:' + startGiorno + ', endOraZ:' + endOraZ)
      print('end giorno:' + endGiorno + ', endOraZ: ' + endOraZ)

      range=pandas.period_range(start=startGiorno+'T'+startOraZ, end=endGiorno+'T'+endOraZ, freq='h')
      #print(range)

      for h in range:
         testGiorno=h.strftime('%Y-%m-%d')
         testOraZ=h.strftime('%H').zfill(2)
         #print('testing ',testGiorno, testOraZ)
         dfTestOra=dfJobsSummary.loc[ ( dfJobsSummary[('giorno')] == testGiorno ) & ( dfJobsSummary[('oraZ')] == testOraZ ) ]
         if len(dfTestOra.index) == 0:
            print ('GIORNO - ORA Z MISSING: ', testGiorno, testOraZ)     


      dfJobsAlert=dfJobsSummary.loc[
         ( dfJobsSummary[('jobId', 'count')] != batchJobsNumber2test )
         |  ( dfJobsSummary[('minuto', 'max')] > numberOfMinutesDelay4error) 
         |  ( dfJobsSummary[('jobState', 'join')].str.contains('failed') ) 
         ]
      print('\n CHECK COINTEGGI JOB ORARI, DELAY, ERRORI:')
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

pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/getComputePods.sas'

out=restApi.runJobExecution(baseUrl,token,contentPathJobExRunPgm,"pgm_url=" + pgmUrl)

elapsed=out["elapsedMs"]
httpStatusCode=out["httpStatusCode"]
Description=out["Description"]
traceBackText=out['Traceback']
response=out["Response"]
   
print("httpStatusCode", httpStatusCode)
print('Ms: ', elapsed)
   
if ( httpStatusCode != 200):
   print('Description',Description)
   if (Description=='GENERIC_ERROR'):
      print('Traceback',traceBackText)

print('*** runJobExecution response: *** \n '+response)

print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





