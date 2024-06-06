

from datetime import datetime
from datetime import date, timedelta

#per prendere datetime.UTC
import datetime as dt    


import pandas as pd


dfEvents = pd.DataFrame({
     'event_timestamp': []
    ,'event_name': []
    ,'event_severity': []
    ,'event_details':[]
})

dfMeasures = pd.DataFrame({
     'measure_timestamp': []
    ,'measure_name':[]
    ,'measure_environment':[]
    ,'measure_value':[]
    ,'measure_desc':[]
})

'''
# dictionary with the data for the new row
newMeasure = {
     'measure_timestamp': datetime.now(dt.UTC)
    ,'measure_name': measure.measureName
    ,'measure_environment': measure.environment
    ,'measure_value': measure.measureValue
    ,'measure_desc': measure.desc
    }
'''

newMeasure = {
     'measure_timestamp': datetime.now(dt.UTC)
    ,'measure_name': 'measureName'
    ,'measure_environment': 'environment'
    ,'measure_value': 'measureValue'
    ,'measure_desc': 'desc'
    }

# Append the dictionary to the DataFrame
dfMeasures.loc[len(dfMeasures)] = newMeasure
# Reset the index
dfMeasures = dfMeasures.reset_index(drop=True)

print('dfMeasures',dfMeasures)
print('dfEvents',dfEvents)