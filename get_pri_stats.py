import pandas as pd
from datetime import datetime
from math import ceil
from copy import deepcopy

FILE = 'October_2023.csv'
TEST_FILE = 'October_2023.csv'
START_TIME = 'dateTimeConnect'
END_TIME = 'dateTimeDisconnect'
ORIGINATION_TIME = 'dateTimeOrigination'
CALL_ID = 'globalCallID_callId'
CALLING_DEVICE = 'origDeviceName'
CALLED_DEVICE = 'destDeviceName'
RUN_AUTOMATICALLY = False

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
            if call_connect == call_disconnect or call_id == 0 or call_connect == 0 or call_disconnect == 0:
                i += 1
                continue
            if calling_device in PRIS:
                call_row = pd.DataFrame([[call_id, call_connect, call_disconnect]], columns=['call_id', 'call_connect', 'call_disconnect'])
                pri_calls = pd.concat([pri_calls, call_row])
            if called_device in PRIS:
                call_row = pd.DataFrame([[call_id, call_connect, call_disconnect]], columns=['call_id', 'call_connect', 'call_disconnect'])
                pri_calls = pd.concat([pri_calls, call_row])
            i += 1
        pri_calls.sort_values('call_connect', inplace=True)
        pri_calls.reset_index(inplace=True, drop=True)
        return pri_calls



def get_active_count(time_stamp):
    i = master_list.start_point
    next_start = 0
    active_calls = 0
    while i < len(master_list.call_list):
        start_time, end_time = master_list.call_list['call_connect'][i], master_list.call_list['call_disconnect'][i]
        if end_time < time_stamp:
            next_start = i
        if start_time <= time_stamp and end_time >= time_stamp:
            active_calls += 1
        if start_time > time_stamp:
            break
        i += 1
    master_list.start_point = next_start
    return active_calls



def process_data():
    call_count = len(master_list.call_list)
    print(f'All calls in list = {call_count}')
    start_time = master_list.call_list['call_connect'].min()
    end_time = master_list.call_list['call_disconnect'].max()
    time_stamp = start_time
    active_calls_csv = pd.DataFrame()
    i = 0
    amount_of_seconds = end_time - start_time
    max_per_minute = 0
    while time_stamp < end_time + 1:
        if i != 0 and i % 5000 == 0:
            print(f'Checking {int(i / 60)} out of {int(amount_of_seconds / 60)} minutes worth of calls')
        active_calls = get_active_count(time_stamp)
        if active_calls > max_per_minute:
            max_per_minute = active_calls
        if i != 0 and i % seconds_to_analyze == 0:
            minutes_row = pd.DataFrame([[datetime.fromtimestamp(time_stamp), time_stamp, max_per_minute]], columns=['time_stamp', 'time_stamp_unix', 'active_calls'])
            max_per_minute = 0
            active_calls_csv = pd.concat([active_calls_csv, minutes_row])
        time_stamp += 1
        i += 1
    return active_calls_csv




def run_automatically():
    global master_list
    FILE = input('Enter the filename you would like to analyze: ')
    if FILE == '':
        FILE = TEST_FILE
    minutes_to_analyze = input('Enter the number of minutes per row you wish to calculate (Default 1) : ')
    if minutes_to_analyze == '':
        minutes_to_analyze = 1
    minutes_to_analyze = int(minutes_to_analyze)
    seconds_to_analyze = minutes_to_analyze * 60
    master_list = AllCallsList()
    active_calls_csv = process_data()
    active_calls_csv.to_csv('analyzed.csv', index=False)

if RUN_AUTOMATICALLY:
    run_automatically()

