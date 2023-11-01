import pandas as pd
from datetime import datetime
from math import ceil
from copy import deepcopy

FILE = 'October_2023.csv'
START_TIME = 'dateTimeConnect'
END_TIME = 'dateTimeDisconnect'
ORIGINATION_TIME = 'dateTimeOrigination'
CALL_ID = 'globalCallID_callId'
CALLING_DEVICE = 'origDeviceName'
CALLED_DEVICE = 'destDeviceName'


PRIS = [
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


class AllCallsList:
    def __init__(self):
        self.call_list = self.get_call_list()
        self.start_point = 0
    def get_call_list(self):
        print('Generating DataFrame of PRI CALLS')
        print('This takes a few minutes, please be patient.')
        cdr_data = pd.read_csv(FILE, low_memory=False)
        # Remove unneeded columns.  Even though we only pass through this list once, theres no point in slowing things down any further.
        cdr_data = cdr_data[[CALL_ID, START_TIME, END_TIME, ORIGINATION_TIME, CALLING_DEVICE, CALLED_DEVICE]]
        pri_calls = pd.DataFrame()
        i = 0
        while i < len(cdr_data):
            call_id, call_connect, call_disconnect, call_origination, calling_device, called_device = cdr_data[CALL_ID][i], cdr_data[START_TIME][i], cdr_data[END_TIME][i], cdr_data[ORIGINATION_TIME][i], cdr_data[CALLING_DEVICE][i], cdr_data[CALLED_DEVICE][i]
            if call_connect == 0:
                call_connect = call_origination
            if call_connect == call_disconnect:
                i += 1
                continue
            call_row = pd.DataFrame([[call_id, call_connect, call_disconnect]], columns=['call_id', 'call_connect', 'call_disconnect'])
            if calling_device in PRIS:
                pri_calls = pd.concat([pri_calls, call_row])
            if called_device in PRIS:
                pri_calls = pd.concat([pri_calls, call_row])
            i += 1
        pri_calls.sort_values('call_connect', inplace=True)
        pri_calls.reset_index(inplace=True, drop=True)
        return pri_calls

master_list = AllCallsList()






def get_calls_that_occured_during(start_time, end_time):
    i = master_list.start_point
    if debug:
        i = 0
    matches = []
    next_start_point = 0
    while i < len(master_list.call_list):
        check_start_time = master_list.call_list['call_connect'][i]
        check_end_time = master_list.call_list['call_connect'][i]
        # We're checking where in the list calls start before our referenced call end.  And that we pass wont need to be checked on the next sweep
        # since the start time should always increment.  We'll set this back in our class at the end of this function.
        if check_end_time < start_time:
            next_start_point = i
        # Now we need to see which calls occur within the referenced call, and count them.
        if check_start_time >= start_time and check_start_time <= end_time: # Start during
            matches.append(i)
        elif check_end_time >= start_time and check_end_time <= end_time: # End During
            matches.append(i)
        elif check_start_time < start_time and check_end_time > end_time: # Start before and end after
            matches.append(i)
        #Once we reach a point where calls are starting after our referenced end time, we can stop, since no other calls would be in the window.
        if check_start_time > end_time:
            break
        i += 1
    master_list.start_point = next_start_point
    results = master_list.call_list.iloc[matches]
    results.reset_index(inplace=True, drop=True)
    return results

def get_active_call_count(start_time, end_time):
    calls_during_window = get_calls_that_occured_during(start_time, end_time)
    max_count = 0
    i = 0
    start_check, end_check = calls_during_window['call_connect'].min(), calls_during_window['call_disconnect'].max()
    while start_check > end_check + 1:
        current_count = len(calls_during_window)
        i = 0
        while i < len(calls_during_window):
            call_start, call_end = calls_during_window['call_connect'][i], calls_during_window['call_disconnect'][i]
            if not call_start <= start_check or call_end >= end_check:
                current_count -= 1
            if current_count >= max_count:
                max_count = current_count
            i += 1
        start_check += 1
    return max_count






def create_csv_with_active_call_count():
    check_list = master_list.call_list
    compiled_csv = pd.DataFrame()
    i = 0
    while i < len(check_list):
        if i != 0 and i % 1000 == 0:
            print(f'Checked {i} out of {len(check_list)} records.')
        call_id, start_time, end_time = check_list['call_id'][i], check_list['call_connect'][i], check_list['call_disconnect'][i]
        # skip garbage data
        if call_id == 0 or start_time == 0 or end_time == 0:
            i += 1
            continue
        active_calls_count = check_call_active_count(start_time, end_time)
        call_row = pd.DataFrame([[call_id, datetime.fromtimestamp(start_time), datetime.fromtimestamp(end_time), active_calls_count, start_time, end_time]], columns=['call_id', 'start_time', 'end_time', 'active_calls', 'start_unix', 'end_unix'])
        compiled_csv = pd.concat([compiled_csv, call_row])
        i += 1
    peak_calls_active = compiled_csv['active_calls'].max()
    print(f'Peak pri usage = {peak_calls_active} calls.')
    return compiled_csv





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