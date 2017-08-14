# -*- coding: utf-8 -*-

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import sys

def multiple_days_uuid(log):
    log['dom']= log['ts'].dt.day # get days of month
    x = log.groupby(['uuid','dom']).size().reset_index().rename(columns={0:'multiple_days'}) #unique uuid and dom combinations
    x = x[x.duplicated('uuid', keep=False)] # not unique uuid
    x = x.drop_duplicates(subset = 'uuid',keep='first')
    x.drop(['dom'], axis=1, inplace=True)
    x['multiple_days'] = 'TRUE'
    return x 

def week_days_uuid(log):
    log.index = log['ts']
    y = log[log['ts'].dt.dayofweek < 5].between_time('9:00','17:00') # get weekdays and filter business time
    y['ts'] = 'TRUE'
    y.columns = ['uuid', 'weekday_biz']
    return y.drop_duplicates()

def get_count(log):
    third = log.groupby('uuid').count() # third feature count of total logins
    return np.array(third['ts'])


def main():
    
    data = pd.read_csv(sys.argv[1],usecols=['uuid','ts'])

    data['ts'] = pd.to_datetime(data['ts'])
 
    first = multiple_days_uuid(data) # first feature
    second = week_days_uuid(data[['uuid','ts']]) # second feature
    third = get_count(data)

    multiple_days_or_weekday = pd.merge(first, second,  how='outer', left_on=['uuid'], right_on =['uuid'] ).fillna('False') # have different day or weekday
    data = data[['uuid']].drop_duplicates() # get orignal list of uuid
    
    
    final_logs = pd.merge(data, multiple_days_or_weekday,  how='outer', left_on=['uuid'], right_on =['uuid'] ).fillna('False').sort_values(by='uuid')
    final_logs['count'] = third # third feature
    
    final_logs.to_csv(path_or_buf= sys.argv[2], index = False) # save csv
        
if __name__ == "__main__":
    main()
