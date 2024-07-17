
import requests
import json
import pandas
import csv


from datetime import datetime
from datetime import date, timedelta
from time import sleep

import os
import subprocess


sessionPath=os.getcwd()

baseUrl = 'https://snamprodukjob.ondemand.sas.com'
flt_env='COB'

baseUrl = 'https://snamprodgerjob.ondemand.sas.com'
flt_env='PRODSUP'



baseUrl = 'https://snamprodmp.ondemand.sas.com'
flt_env='PROD'

baseUrl = 'https://snamtest.ondemand.sas.com'
flt_env='TEST'

tokenFile='.\\dat\\blob_storage_tokens.csv'


dfTokens = pandas.read_csv(tokenFile, sep=';')
#print(dfTokens) 


flt_inout='outgoing'
flt_inout='incoming'



flt_scope='SAS'
flt_scope='CUSTOMER'


dfToken = dfTokens [ (dfTokens['env']==flt_env) & (dfTokens['inout']==flt_inout) & (dfTokens['scope']==flt_scope) ]
print(dfToken) 

containerUrl=dfToken['containerUrl'].values[0]
token=dfToken['token'].values[0]
#print(containerUrl)
#print(token)

print('\n')
print('Path sessione corrente:')
print(sessionPath)



if (flt_inout == 'incoming') :

    file2loadLocalName='hcFolder.tar'
    file2loadRemoteName='hcFile_2024_21_05.txt'
    apice='"'
    cmdCopyFile='azcopy copy ' + file2loadLocalName + ' ' + apice + containerUrl + '/' + file2loadRemoteName + '?' + token + apice
    print('\n')
    print('COMANDO COPIA FILE:')
    print(cmdCopyFile)
    
    folder2loadName='hcFolder'
    apice='"'
    cmdCopyFolder='azcopy copy ' + apice + folder2loadName+apice + ' ' + apice + containerUrl + '?' + token + apice + ' --recursive'
    print('\n')
    print('COMANDO COPIA FOLDER:')
    print(cmdCopyFolder)


if (flt_inout == 'outgoing') :
    file2downloadRemoteName='hcFile_2024_19_05.txt'
    file2downloadLocalName='downloaded_hcFile.txt'
  
  
    apice='"'
    cmdCopyFile='azcopy copy ' + apice + containerUrl + '/' + file2downloadRemoteName + '?' + token + apice + ' ' + file2downloadLocalName   
    print('\n')
    print('COMANDO DOWNLOAD  FILE:')
    print(cmdCopyFile)

    apice='"'
    cmdRemoveFile='azcopy rm ' + apice + containerUrl + '/' + file2downloadRemoteName + '?' + token + apice    
    print('\n')
    print('COMANDO DELETE REMOTE FILE:')
    print(cmdRemoveFile)
    
    folder2downloadRemoteName='hcFolder'
    folder2downloadLocalPath='.\\'
    apice='"'
    cmdCopyFolder='azcopy copy ' + apice + containerUrl + '/' + folder2downloadRemoteName + '?' + token + apice + ' ' + folder2downloadLocalPath  +  ' --recursive'
    print('\n')
    print('COMANDO DOWNLOAD FOLDER:')
    print(cmdCopyFolder)

    apice='"'
    cmdRemoveFolder='azcopy rm ' + apice + containerUrl + '/' + folder2downloadRemoteName + '?' + token + apice  + ' --recursive'
    print('\n')
    print('COMANDO DELETE REMOTE FOLDER:')
    print(cmdRemoveFolder)


# circa 5 min per azcopy pura

print('\n')
print('COMANDO LIST:')
cmdList='azcopy list ' + apice + containerUrl + '?' + token + apice
print(cmdList)

#os.system(cmdList)

'''
try:
    result = subprocess.check_output(cmdList, shell=True, text=True)
    print(result)
except subprocess.CalledProcessError as e:
    print(f"Error executing command: {e}")
'''

