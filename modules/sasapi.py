

import requests
import json

from datetime import datetime
from datetime import date, timedelta
import traceback 
import sys


def getToken(baseUrl):
   url = baseUrl + "/SASLogon/oauth/token"
   payload = 'grant_type=password&username=itamrz&password=Vitamina.002'
   headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Authorization': 'Basic c2FzLmVjOg=='
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



def getComputeContexts(baseUrl,token):
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

def getContextDefinitionAttributes(baseUrl,contextId,token):
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



def getJobs(baseUrl,token,lastDaysNumber):
   #url='https://snamprodgerjob.ondemand.sas.com/jobExecution/jobs?start=0&limit=20&sortBy=creationTimeStamp:descending&filter=and(ge(creationTimeStamp,%272024-04-25T02:56:54.277Z%27),lt(creationTimeStamp,%272024-04-26T02:56:54.277Z%27))'
   #url='https://snamprodgerjob.ondemand.sas.com/jobExecution/jobs?start=0&limit=400&sortBy=creationTimeStamp:descending&filter=and(ge(creationTimeStamp,%272024-04-25T20:00:00.000Z%27))'
   #url='https://snamprodgerjob.ondemand.sas.com/jobExecution/jobs?start=0&limit=1000&sortBy=creationTimeStamp:descending&filter=and(ge(creationTimeStamp,%272024-04-25T22:00:00.000Z%27),lt(creationTimeStamp,%272024-04-30T23:59:59.999Z%27))'
   #url = baseUrl + "/jobExecution/jobs?limit=1000"

   dateStart = date.today() - timedelta(lastDaysNumber)
   print('Current Date :', date.today())
   print(' dateStart :', dateStart) 

   url = baseUrl + "/jobExecution/jobs?start=0&limit=4000&sortBy=creationTimeStamp:ascending&filter=and(ge(creationTimeStamp,%27" + dateStart.strftime("%Y-%m-%d") + "T00:00:00.000Z%27))"
   #url = baseUrl + "/jobExecution/jobs?limit=1000"
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


def runJobExecution(baseUrl,token,program,parms):
   
   url = baseUrl + "/SASJobExecution/?_program=" + program
   if ( parms != '' ): 
      url = url + "&" + parms

   payload = {}
   headers = {
      #'Accept': 'application/vnd.sas.api+json',
      'Authorization': token
      }
   try:
        response = requests.request("GET", url, headers=headers, data=payload, allow_redirects=True, timeout=90)
        #print(response.text)
        return response
   except requests.exceptions.Timeout:
    print("timeout")
    return {'ERROR: Timeout'}
 

def getConfigurationDefinition(baseUrl,token,definitionItem):
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
