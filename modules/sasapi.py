

import requests
import json

from datetime import datetime
from datetime import date, timedelta
#per prendere datetime.UTC
import datetime as dt   

import traceback 
import sys

import logging
import configparser


class Measure:
   def __init__(self, environment, timestamp, measureName, measureValue, desc):
    self.environment = environment
    self.timestamp = timestamp
    self.measureName=measureName
    self.measureValue=measureValue
    self.desc=desc


class Stats:
   sep=';'
   def __init__(self,logger):
       self.logger=logger
      
   def handleMeasure(self,measure):

      defaultMsg=measure.environment + self.sep + measure.measureName + '=' + measure.measureValue + self.sep + measure.desc
      
      match measure.measureName:
         case 'QUERY_MISURATORI_SCADA_ERROR':
            self.logger.critical(defaultMsg)
         case 'GET_SNM_PCT_USED_ERROR':
            self.logger.critical(defaultMsg)
         case 'GET_NUM_COMPUTE_PODS_ERROR':
            self.logger.critical(defaultMsg)
         case 'GET_JOBS_EMPTY':
            self.logger.error(defaultMsg)
         case 'CHECK_JOBS_HOURLY':
            if ( measure.desc != 'OK' ):
               self.logger.critical(defaultMsg)
            else:
               self.logger.info(defaultMsg)
         case 'SNM_PCT_USED':
            if int(measure.measureValue) <= 10:
               self.logger.info(defaultMsg)
            elif int(measure.measureValue) > 10 and int(measure.measureValue) < 50:
               self.logger.warning(defaultMsg)
            elif int(measure.measureValue) >= 50:
               self.logger.error(defaultMsg)      
         case 'NUM_COMPUTE_PODS':
            if int(measure.measureValue) <= 10:
               self.logger.info(defaultMsg)
            elif int(measure.measureValue) > 10 and int(measure.measureValue) < 100:
               self.logger.warning(defaultMsg)
            elif int(measure.measureValue) >= 100 and int(measure.measureValue) < 200:
               self.logger.error(defaultMsg)
            elif int(measure.measureValue) >= 200:
               self.logger.critical(defaultMsg)
         case 'QUERY_MISURATORI_SCADA_COUNT':
            if int(measure.measureValue) <= 10:
               self.logger.error(defaultMsg)
            elif int(measure.measureValue) > 10 and int(measure.measureValue) < 5000:
               self.logger.warning(defaultMsg)
            elif int(measure.measureValue) >= 5000:
               self.logger.info(defaultMsg)     
         case "GET_TOKEN_ERROR":
            self.logger.critical(defaultMsg)
         case "GET_TOKEN_ELAPSED":
            if float(measure.measureValue) > 600:
               self.logger.warning(defaultMsg)
            else:
               self.logger.info(defaultMsg)
         case _:
            self.logger.info(defaultMsg)


class RestApi:

   def __init__(self, logger, cfgFile):
       
       self.logger=logger

       config = configparser.ConfigParser()
       config.read(cfgFile)

       self.api_client_basic_authorization = config.get('app client', 'basic_authorization')
       self.api_client_refresh_token = config.get('app client', 'refresh_token')
       
       self.default_api_client_username = config.get('default client', 'username')
       self.default_api_client_password = config.get('default client', 'password')
       self.default_api_client_basic_authorization = config.get('default client', 'basic_authorization')

       self.emailsFrom = config.get('email', 'emails_from')

       self.jobexec_pgm_from_url=config.get('jobs', 'jobexec_pgm_from_url')


       '''
       print(self.api_client_basic_authorization)
       print(self.api_client_refresh_token)
       print(self.default_api_client_username)
       print(self.default_api_client_password)
       print(self.default_api_client_basic_authorization)
       print(self.emailsFrom)
       print(self.jobexec_pgm_from_url)
       '''
   
   def getToken(self,baseUrl):
      
      url = baseUrl + "/SASLogon/oauth/token"

      if (baseUrl != 'https://snamtest.ondemand.sas.com'):
         payload = 'grant_type=password&username='+ self.default_api_client_username +'&password=' + self.default_api_client_password
         headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + self.default_api_client_basic_authorization
            }
      else:
         '''   
         # non legge i jobs
         payload = 'grant_type=client_credentials'
         headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ............................'
            }  
         '''
         # con il refresh token invece li legge
         #rt='eyJhbGciOiJSUzI1NiIsImprdSI6Imh0dHBzOi8vbG9jYWxob3N0L1NBU0xvZ29uL3Rva2VuX2tleXMiLCJraWQiOiJsZWdhY3ktdG9rZW4ta2V5IiwidHlwIjoiSldUIn0.eyJqdGkiOiI1MDI1MTQ0NDE0NGE0ZTBiOTg3MjBkYmM1Mzc4MzM2Yi1yIiwic3ViIjoiNDg1YjNjMjItNTU0Ny00OGI1LWFjNWQtMzdmMTg2NWVjZmRkIiwiaWF0IjoxNzE3NTA5NDMxLCJleHAiOjIxOTA1NDk0MzEsImNpZCI6InNhcy5jbGllbnRfZ3QzIiwiY2xpZW50X2lkIjoic2FzLmNsaWVudF9ndDMiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0L1NBU0xvZ29uL29hdXRoL3Rva2VuIiwiemlkIjoidWFhIiwiYXVkIjpbIm9wZW5pZCIsInNhcy5jbGllbnRfZ3QzIl0sImdyYW50ZWRfc2NvcGVzIjpbIm9wZW5pZCJdLCJhbXIiOlsiZXh0IiwicHdkIl0sImF1dGhfdGltZSI6MTcxNzUwOTQzMSwiYXV0aG9yaXRpZXMiOlsiU05NIERldmVsb3BlcnMiLCJzbm1hcHAiLCJTQVNTY29yZVVzZXJzIiwiRGF0YUJ1aWxkZXJzIiwiQXBwbGljYXRpb25BZG1pbmlzdHJhdG9ycyIsIkVzcmlVc2VycyIsIlNOTV9TY2hlZHVsZV9Hcm91cCJdLCJleHRfaWQiOiJjbj1pdGFtcnosb3U9VXNlcnMsb3U9U0FTLGRjPXZzcCxkYz1zYXMsZGM9Y29tIiwiZ3JhbnRfdHlwZSI6InBhc3N3b3JkIiwidXNlcl9uYW1lIjoiaXRhbXJ6Iiwib3JpZ2luIjoibGRhcCIsInVzZXJfaWQiOiI0ODViM2MyMi01NTQ3LTQ4YjUtYWM1ZC0zN2YxODY1ZWNmZGQiLCJyZXZfc2lnIjoiNzM1MjAxM2EifQ.ZvtUmIwZ6JVG75dFLfELEYJBF_sT4AnfOhEoccgfTUO5gBgqinwMud9Wj6YKVgl6N3zjINgLhCIuwFXHwWE3LPt1YZ5AWO7Ryh0-hEg_XBrpIFMmU8C806x5myrS068-axAMkjomPDcc9ibSdFH3VmxJsY1SWp0Q4lAy7HHvzagGgIFvD-3DtRiWaCKsmdlO6pDS_3sfB4cJoPxxOIMV2tsLsMXoNScbzVAdTxipzGC_AIANi_4ImSDAQDKeWXXepcvsFLe1a-A4VEBcQ4vg69rE6UEc9wUWkRH9ps5_h887TGYvwnLFMyo7PX2FjFYbLrLc-lVHDUVcKmPxcFarEA'
         payload = 'grant_type=refresh_token&refresh_token=' + self.api_client_refresh_token
         headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + self.api_client_basic_authorization
            }  
         
      outDesc='Ko'
      startDt=datetime.now()
      endDt=datetime.now()
      responseStatusCode=-1
      responseText=''
      token='error'
      traceBackText=''

      try:
         response = requests.request("POST", url, headers=headers, data=payload, timeout=30)
         responseStatusCode=response.status_code
         responseText=response.text
         endDt=datetime.now()
      except requests.exceptions.Timeout:
         outDesc='TIMEOUT'
         traceBackText=traceback.format_exception(*sys.exc_info())
      except requests.exceptions.ProxyError:
         outDesc='PROXY_ERROR'
         traceBackText=traceback.format_exception(*sys.exc_info())
      except Exception as e:
         outDesc='GENERIC_ERROR'
         traceBackText=traceback.format_exception(*sys.exc_info())
         
      elapsed=(endDt-startDt).microseconds/1000
      #print('elapsed millisecondi: ', elapsed)
      #print('response.status_code=', responseStatusCode)
      if (responseStatusCode == 200): 
         rj = json.loads(responseText)
         outDesc='Ok'
         token='Bearer ' + rj['access_token']
      
      #print(token)

      out={"token": token, 
         "elapsedMs": elapsed,
         "httpStatusCode": responseStatusCode,
         "Response": responseText,
         "Description": outDesc,
         "Traceback": traceBackText
         }
      #return token
      return out

   def getComputeContexts(self,baseUrl,token):
      payload = {}
      headers = {
         'Authorization': token
         }
      url = baseUrl + "/compute/contexts"
      response = requests.request("GET", url, headers=headers, data=payload)
      #print(response.text)
      rj = json.loads(response.text)
      items=rj['items']
      #print(items)
      return items

   def getContextDefinitionAttributes(self,baseUrl,contextId,token):
      payload = {}
      headers = {
         'Authorization': token
         }
      url = baseUrl + "/compute/contexts/"+contextId
      response = requests.request("GET", url, headers=headers, data=payload)
      #print(response.text)
      rj = json.loads(response.text)
      contextAttributes=rj['attributes']
      return contextAttributes



   def getJobs(self,baseUrl,token,lastHoursNumber):
      #url='https://snamprodgerjob.ondemand.sas.com/jobExecution/jobs?start=0&limit=20&sortBy=creationTimeStamp:descending&filter=and(ge(creationTimeStamp,%272024-04-25T02:56:54.277Z%27),lt(creationTimeStamp,%272024-04-26T02:56:54.277Z%27))'
      #url='https://snamprodgerjob.ondemand.sas.com/jobExecution/jobs?start=0&limit=400&sortBy=creationTimeStamp:descending&filter=and(ge(creationTimeStamp,%272024-04-25T20:00:00.000Z%27))'
      #url='https://snamprodgerjob.ondemand.sas.com/jobExecution/jobs?start=0&limit=1000&sortBy=creationTimeStamp:descending&filter=and(ge(creationTimeStamp,%272024-04-25T22:00:00.000Z%27),lt(creationTimeStamp,%272024-04-30T23:59:59.999Z%27))'
      #url = baseUrl + "/jobExecution/jobs?limit=1000"

   
      dtNow=datetime.now(dt.UTC)
      #print(dtNow)
      dtStart = dtNow - timedelta(hours=lastHoursNumber)
      #print(dtStart)

      url = baseUrl + "/jobExecution/jobs?start=0&limit=4000&sortBy=creationTimeStamp:ascending&filter=and(ge(creationTimeStamp,%27" + dtStart.strftime("%Y-%m-%dT%H:%M:%S.000Z") + "%27))"
      print (url)

      payload = {}
      headers = {
         'Content-Type': 'application/vnd.sas.collection+json',
         'Accept': 'application/vnd.sas.collection+json',
         'Authorization': token
         }
      try:
         response = requests.request("GET", url, headers=headers, data=payload, allow_redirects=True)
         print('response.status_code=', response.status_code)
      except requests.exceptions.Timeout:
         print("timeout")
      #print(response.text)
      rj = json.loads(response.text)
      items=rj['items']
      #print(items)
      return items


   def runJobExecution(self,baseUrl,token,program,parms):
      
      url = baseUrl + "/SASJobExecution/?_program=" + program
      if ( parms != '' ): 
         url = url + "&" + parms
      #print(url)

      outDesc='Ko'
      startDt=datetime.now()
      endDt=datetime.now()
      responseStatusCode=-1
      responseText=''
      traceBackText=''

      payload = {}
      headers = {
         #'Accept': 'application/vnd.sas.api+json',
         'Authorization': token
         }
      try:
         response = requests.request("GET", url, headers=headers, data=payload, allow_redirects=True, timeout=90)
         responseStatusCode=response.status_code
         responseText=response.text
         endDt=datetime.now()

      except requests.exceptions.Timeout:
         traceBackText=traceback.format_exception(*sys.exc_info())
         outDesc='TIMEOUT'
       
      except Exception as e:
         outDesc='GENERIC_ERROR'
         traceBackText=traceback.format_exception(*sys.exc_info())
         
      elapsed=(endDt-startDt).microseconds/1000
      #print('elapsed millisecondi: ', elapsed)
      #print('response.status_code=', responseStatusCode)
      if (responseStatusCode == 200): 
          outDesc='Ok'

      out={
         "elapsedMs": elapsed,
         "httpStatusCode": responseStatusCode,
         "Response": responseText,
         "Description": outDesc,
         "Traceback": traceBackText
         }

      return out
   

   def getConfigurationDefinition(self,baseUrl,token,definitionItem):
      payload = {}
      headers = {
         'Authorization': token
         }
      url = baseUrl + "/configuration/configurations?definitionName="+definitionItem
      response = requests.request("GET", url, headers=headers, data=payload)
      #print(response.text)
      rj = json.loads(response.text)
      items=rj['items']
      #print(items)
      return items




   def buildJobsDataTable(self,items):
      
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
         #print(jobRequest)
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
            #| 
            # job schedulati
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
         
      return jobsdata
   
   def sendMail(self,baseUrl,token, toList,subject,body):

      pgmUrl = 'https://raw.githubusercontent.com/marcoZav/HCpy/main/jobex/sendMail.sas'

      parms='toList:' + toList + '|sender:'+ self.emailsFrom + '|subject:'+ subject +'|body:' + body
      print(parms)

      # exec_pgm_from_url ha come parametri: pgm_url=, parms=
      
      resp=self.runJobExecution(baseUrl,token,self.jobexec_pgm_from_url,"pgm_url=" + pgmUrl + '&parms='+ parms)
      #print(resp)
      return resp