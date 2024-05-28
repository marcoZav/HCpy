
import requests
import json
import pandas
import sys

from datetime import datetime
from datetime import date, timedelta
from time import sleep




baseUrl = 'https://snamprodukjob.ondemand.sas.com'
baseUrl = 'https://snamprodmp.ondemand.sas.com'
baseUrl = 'https://snamtest.ondemand.sas.com'
baseUrl = 'https://snamprodgerjob.ondemand.sas.com'



def getToken(baseUrl):
   url = baseUrl + "/SASLogon/oauth/token"
   payload = 'grant_type=password&username=itamrz&password=Vitamina.002'
   headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Authorization': 'Basic c2FzLmVjOg=='
  }
   response = requests.request("POST", url, headers=headers, data=payload)
   print('response.status_code=', response.status_code)
   if (response.status_code != 200): 
      print(response.text)
   rj = json.loads(response.text)
   token='Bearer ' + rj['access_token']
   #print(token)
   return token



def runJobExecution(baseurl,program,parms):
   
   url = baseUrl + "/SASJobExecution/?_program=" + program;
   if ( parms != '' ): 
      url = url + "&" + parms
   print(url)

   payload = {}
   headers = {
      #'Accept': 'application/vnd.sas.api+json',
      'Authorization': token
      }
   try:
        response = requests.request("GET", url, headers=headers, data=payload, allow_redirects=True)
        #print(response.text)
        return response
   except requests.exceptions.Timeout:
    #print("timeout")
    return {'ERROR: Timeout'}
 


print('START ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------------------------------------------------------------------------')
print (baseUrl)

print('\n')
print('-- Get Token:')

token=getToken(baseUrl)
#print(token)
print('TOKEN ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' ----------------------------')





print('\n')
print('-- RUN Job Execution:')

pgmUrl = 'https://raw.githubusercontent.com/marcoZav/opsMng/main/jobex_test_parallel01'

# argv[0] è il nome del programma .py, parametri iniziano da 1
lp=len(sys.argv)
print(lp)

if (lp>1):
   jnum=sys.argv[1]   
else:
   jnum='00'

print (jnum)

# poi sarebbe da scrivere meglio con le urlencode
#  :  %3A
response=runJobExecution(baseUrl
                         ,'%2FSNM%2Futility_jobs%2Fexec_pgm_from_url'
                         ,"pgm_url=" + pgmUrl + '&parms=jnum%3A'+jnum+'|sleepSeconds%3A5' )
print(response.text)

print('END ' + datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + '----------------------------------------------------------------------------------------------')





