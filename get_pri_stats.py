import pandas as pd
from datetime import datetime
from math import ceil
from copy import deepcopy

FILE = 'October_2023.csv'
START_TIME = 'dateTimeConnect'
OTHER_START = 'dateTimeOrigination'
END_TIME = 'dateTimeDisconnect'
CALL_ID = 'globalCallID_callId'
CALLING_DEVICE = 'origDeviceName'
CALLED_DEVICE = 'destDeviceName'



pris = [
'S0/SU1/DS1-0@gw6-voip.jpl.nasa.gov',
'S0/SU1/DS1-0@gw7-voip.jpl.nasa.gov',
'S0/SU1/DS1-1@gw7-voip.jpl.nasa.gov',
'S0/SU1/DS1-2@gw6-voip.jpl.nasa.gov',
'S0/SU1/DS1-2@gw7-voip.jpl.nasa.gov',
'S0/SU1/DS1-3@gw6-voip.jpl.nasa.gov',
'S0/SU1/DS1-3@gw7-voip.jpl.nasa.gov',
'S0/SU1/DS1-4@gw6-voip.jpl.nasa.gov',
'S0/SU1/DS1-4@gw7-voip.jpl.nasa.gov',
'S0/SU1/DS1-5@gw7-voip.jpl.nasa.gov',
'S0/SU1/DS1-7@gw7-voip.jpl.nasa.gov',
'S0/SU2/DS1-0@gw6-voip.jpl.nasa.gov',
'S0/SU2/DS1-0@gw7-voip.jpl.nasa.gov',
'S0/SU2/DS1-1@gw6-voip.jpl.nasa.gov',
'S0/SU2/DS1-1@gw7-voip.jpl.nasa.gov',
'S0/SU2/DS1-2@gw6-voip.jpl.nasa.gov',
'S0/SU2/DS1-2@gw7-voip.jpl.nasa.gov',
'S0/SU2/DS1-3@gw6-voip.jpl.nasa.gov',
'S0/SU2/DS1-3@gw7-voip.jpl.nasa.gov',
'S0/SU2/DS1-4@gw6-voip.jpl.nasa.gov',
'S0/SU2/DS1-4@gw7-voip.jpl.nasa.gov',
'S0/SU2/DS1-5@gw6-voip.jpl.nasa.gov',
'S0/SU2/DS1-5@gw7-voip.jpl.nasa.gov',
'S0/SU2/DS1-7@gw7-voip.jpl.nasa.gov']

def get_call_list():
    cdr_data = pd.read_csv(FILE, low_memory=False)
    i, call_list = 0, []
    while i < len(cdr_data):
        call_id, start_time, end_time, calling_device, called_device, other_start = cdr_data[CALL_ID][i], cdr_data[START_TIME][i], cdr_data[END_TIME][i], cdr_data[CALLING_DEVICE][i], cdr_data[CALLED_DEVICE][i], cdr_data[OTHER_START][i]
        if start_time == 0:
            start_time = other_start
        if start_time != end_time and start_time < end_time:
            minutes = ceil((end_time - start_time) / 60)
            if calling_device in pris:
                call_list.append([call_id, start_time, end_time, minutes])
            if called_device in pris:
                call_list.append([call_id, start_time, end_time, minutes])
        i += 1
    return call_list


def check_active_call(start_time, end_time, df_call_list):
    total_active = 0
    total_active += call_inside_call_boundary(start_time, end_time, df_call_list)


    after = before[before['start_time'] >= end_time]
    active_count = len(after)
    return active_count


def call_inside_call_boundary(start_time, end_time, df_call_list):
    after = df_call_list[df_call_list['start_time'] <= start_time]
    before = after[after['end_time'] >= end_time]
    return len(before)




def get_active_calls():
    active_calls = []
    call_list = get_call_list()
    df_call_list = pd.DataFrame(call_list)
    df_call_list = df_call_list.rename(columns={0: "call_id", 1: "start_time", 2: "end_time", 3: "minutes"})
    df_call_list = df_call_list.sort_values('start_time')
    df_call_list.reset_index(inplace=True, drop=True)
    i = 0
    for call in call_list:
        call_id, start_time, end_time, minutes = call[0], call[1], call[2], call[3]
        if call_id == 0:
            i += 1
            continue
        active_count = call_inside_call_boundary(start_time, end_time, df_call_list)
        #active_count = check_active_call(start_time, end_time, df_call_list)
        active_calls.append([call_id, datetime.fromtimestamp(start_time), minutes, active_count])
        if active_count
        print(i, active_count)
        i += 1
    return active_calls


def test_data():
    max_calls = 0
    for call in active_calls:
        if call[3] > max_calls:
            max_calls = call[3]
    print(max_calls)


def get_composite_data():
    calls_with_date = []
    active_calls = get_active_calls()
    for call in active_calls:
        call_id, time_stamp, minutes, active_calls = call[0], call[1], call[2], call[3]
        day = time_stamp.day
        month = time_stamp.month
        hour = time_stamp.hour
        minute = time_stamp.minute
        calls_with_date.append([call_id, month, day, hour, minute, minutes, active_calls])
    composite_data = pd.DataFrame(calls_with_date)
    composite_data = composite_data.rename(columns={0: "call_id", 1: "month", 2: "day", 3: "hour", 4: "minute", 5: "call_length_minutes", 6: "calls_active"})
    composite_data = composite_data.sort_values('call_id')
    composite_data.reset_index(inplace=True, drop=True)
    return composite_data

def get_hourly_volume(composite_data):
    calls_by_hour = []
    max_day = composite_data['day'].max()
    day_data = 1
    while day_data < max_day + 1:
        day_dataframe = composite_data[composite_data['day'] == day_data]
        hour_data = 0
        while hour_data < 24:
            hour_dataframe = day_dataframe[day_dataframe['hour']==hour_data]
            hour_count = len(hour_dataframe)
            calls_by_hour.append([day_data, hour_data, hour_count])
            hour_data += 1
        day_data += 1
    calls_by_hour = pd.DataFrame(calls_by_hour)
    calls_by_hour = calls_by_hour.rename(columns={0: "day", 1: "hour", 2: "call_volume"})
    return calls_by_hour


def get_max_active(composite_data):
    calls_by_hour = []
    max_usage = 0
    max_day = composite_data['day'].max()
    day_data = 1
    while day_data < max_day + 1:
        day_dataframe = composite_data[composite_data['day'] == day_data]
        hour_data = 0
        while hour_data < 24:
            hour_dataframe = day_dataframe[day_dataframe['hour'] == hour_data].sort_values(by=['call_id'])
            hour_dataframe.reset_index(inplace=True, drop=True)
            calls_by_hour.append([day_data, hour_data, hour_count])
            hour_data += 1
        day_data += 1
    calls_by_hour = pd.DataFrame(calls_by_hour)
    calls_by_hour = calls_by_hour.rename(columns={0: "day", 1: "hour", 2: "call_volume"})
    return calls_by_hour


def process_data():
    composite_data = get_composite_data()
    hourly_call_volume = get_hourly_volume(composite_data)
    composite_data.to_csv('call_details.csv', index=False)
    hourly_call_volume.to_csv('hourly_calls.csv', index=False)


#process_data()