import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import sys


def multiple_days_uuid(log):
    x =log.ix[(logs.duplicated('uuid',keep =False))] # not unique uuid
    x = x.sort_values(by='uuid')
    x['dom']= x['ts'].dt.day # get days of month
    x = x.drop('ts', axis=1)  # drop time stamp
    x = x.drop_duplicates() # drop duplicates means: same day and same uuid but we can not drop all
    x = x[x.duplicated('uuid', keep=False)] # drop the one uuid that remains from last op and  is also same day and same uuid
    multiple_days = x['uuid']  
    x['multiple_days'] = np.ones(multiple_days.shape)  #assign one to all uuid
    x.drop(['dom'], axis=1, inplace=True)  # drop day of month
    x = x.drop_duplicates() # drop multiple uuids with same multiple_days flag
    return x

def week_days_uuid(log):

    y = log[log['ts'].dt.dayofweek < 5] # get weekdays
    y.index = y['ts']
    y = y.between_time('9:00','17:00') # filter business time
    weekday_biz = y['uuid']
    y['weekday_biz'] = np.ones(weekday_biz.shape)
    y.drop(['ts'], axis=1, inplace=True)
    y = y.drop_duplicates()
    return y

def get_count(logs):

    third = logs.groupby('uuid').count() # third feature count of total logins
    third.sort_index(inplace=True)
    third = np.array(third)
    return third



def main():
    
    logs = pd.read_csv(sys.argv[1])
    logs['ts'] = pd.to_datetime(logs['ts'])
    logs = logs[['uuid','ts']]

    first = multiple_days_uuid(logs) # first feature
    sedond = week_days_uuid(logs) # second feature
    third = get_count(logs) # third feature
    
    multiple_days_or_weekday = pd.merge(first, sedond,  how='outer', left_on=['uuid'], right_on =['uuid'] ) # have different day or weekday
    multiple_days_or_weekday = multiple_days_or_weekday.fillna(0)
    
    logs = logs[['uuid']]
    logs = logs.drop_duplicates() # get orignal list of uuid
    
    
    final_logs = pd.merge(logs, multiple_days_or_weekday,  how='outer', left_on=['uuid'], right_on =['uuid'] ) 
    final_logs = final_logs.fillna(0)
    final_logs = final_logs.sort_values(by='uuid') # sort coz we have to add thrid according to uuid
    final_logs['count'] = third
    
    final_logs.to_csv(path_or_buf= sys.argv[2], index = False) # save csv
    
    
    #final_logs.ix[final_logs['multiple_days'] == 1].shape  # total ones in multiple_days 36035
    #final_logs.ix[final_logs['weekday_biz'] == 1].shape   # total ones in weekday_biz 90868
    
if __name__ == "__main__":
    main()    
