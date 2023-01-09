## Written by John Huthmaker
## December 14th, 2022
## This script reads the json file located in the subfolder rotation_list_file.  It backs it up, and then presents
## various options for adjusting the list.  Upon completion, the user is asked if they would like to save the file.
## If they answer affirmatively, the rolation_list.json is overwritten with the updated information.



import json
from datetime import datetime
from time import sleep
from os import system, name
import re

def format_tel(tel):
    tel = tel.removeprefix("+")
    tel = tel.removeprefix("1")     # remove leading +1 or 1
    tel = re.sub("[ ()-]", '', tel) # remove space, (), -

    assert(len(tel) == 10)
    tel = f"{tel[:3]}-{tel[3:6]}-{tel[6:]}"

    return tel

def clear():
    # for windows
    if name == 'nt':
        _ = system('cls')

    # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')

def read_in_json():
    with open('./rotation_list_file/rotation_list.json', 'r') as untouched_file:
        rotation_list = json.loads(untouched_file.read())
    return rotation_list

def write_json_file(json_data, filename):
    with open(filename, 'w') as write_file:
        write_file.write(json.dumps(json_data))


def clean_numbers(rotation_list):
    i = 0
    while i < len(rotation_list['oncallList']):
        number = rotation_list['oncallList'][i]['phoneNumber']
        number = number.replace(' ', '')
        rotation_list['oncallList'][i]['phoneNumber'] = number
        i += 1
    return rotation_list




def start_script():
    clear()
    current_time = datetime.now().strftime("%d_%m_%Y_%H_%M_%S.json")
    rotation_list = read_in_json()
    write_json_file(rotation_list, f'./rotation_list_file/Backups/rotation_list_backup_{current_time}')
    rotation_list = clean_numbers(rotation_list)
    users_numbers = extract_users_numbers(rotation_list)
    message = "You've started up the on call rotation list editor.\n\n\n"
    message += current_list_display(users_numbers)
    print(message)
    menu_options(users_numbers)




def extract_users_numbers(rotation_list):
    users_numbers = []
    i = 1
    for user in rotation_list['oncallList']:
        users_numbers.append([i, user['userName'], format_tel(user['phoneNumber'])])
        i += 1
    return users_numbers


def current_list_display(users_numbers):
    text = "The current rotation list is as follows:\n"
    for user in users_numbers:
        text += f'\nRotation #{user[0]}) Name: {user[1]} | Number {pretty_format(user[2])}'
    return text

def menu_options(users_numbers):
    menu = '\nBelow are the available options:\n\n' \
           'Option 1 - Swap two positions in the list\n' \
           'Option 2 - Remove a person from the list\n' \
           'Option 3 - Add person to list\n' \
           'Option 4 - Change a persons phone number\n' \
           'Option 5 - Name Change\n' \
           'Option 6 - Finish (Save Changes to File)\n\n'
    while True:
        print(menu)
        selected_option = int(input('What would you like to do?: '))
        if selected_option == 1:
            pos1 = int(input('What is the first position? ')) - 1
            pos2 = int(input('What is the second position? ')) - 1
            users_numbers = swap_positions(pos1, pos2, users_numbers)
        elif selected_option == 2:
            pos = int(input('Which person would you like to remove from this list? ')) - 1
            del_position(pos, users_numbers)
        elif selected_option == 3:
            pos = int(input('In what position do you want this new person? (The rest move one number higher on the list) ')) - 1
            name = input('What is the persons name? ')
            number = format_tel(input('What is their phone number? '))
            users_numbers = add_position(pos, name, number, users_numbers)
        elif selected_option == 4:
            pos = int(input('Which position did you want to modify? ')) - 1
            new_number = input('What number do you want to change it to? ')
            users_numbers = change_number(pos, new_number, users_numbers)
        elif selected_option == 5:
            pos = int(input('Which position did you want to modify? ')) - 1
            new_name = input('What is their new name? ')
            users_numbers = change_name(pos, new_name, users_numbers)
        elif selected_option == 6:
            break
        else:
            print('That is not a valid option')
            continue
        users_numbers = renumber_list(users_numbers)
        clear()
        print(current_list_display(users_numbers))
    final_list = recompile_list(users_numbers)
    clear()
    print('\n\n' + current_list_display(users_numbers))
    final_choice = input('\nDo you want to save? ')
    if final_choice.lower()[:1] == 'y':
        write_json_file(final_list, './rotation_list_file/rotation_list.json')
    return


def swap_positions(pos1, pos2, users_numbers):
    position_1_num, position_1_name, position_2_num, position_2_name = users_numbers[pos1][2], users_numbers[pos1][1], users_numbers[pos2][2], users_numbers[pos2][1]
    response = input(f'Do you want to swap {position_1_name} with {position_2_name}? ')
    if response.lower()[:1] == 'y':
        users_numbers[pos2][2], users_numbers[pos2][1], users_numbers[pos1][2], users_numbers[pos1][1] = position_1_num, position_1_name, position_2_num, position_2_name
    return users_numbers

def add_position(pos, name, number, users_numbers):
    current_person = users_numbers[pos][1]
    print(f'{current_person} will move to position {(pos + 2)}, and {name} will move to position {(pos + 1)}')
    response = input(f'Do you want to make this addition? ')
    if response.lower()[:1] == 'y':
        users_numbers.insert(pos, [(pos + 1), name, number])
    return users_numbers


def change_name(pos, new_name, users_numbers):
    previous_name = users_numbers[pos][1]
    response = input(f"Do you want to change {previous_name}'s name to {new_name} ")
    if response.lower()[:1] == 'y':
        users_numbers[pos][1] = new_name
    return users_numbers

def renumber_list(users_numbers):
    i = 0
    while i < len(users_numbers):
        users_numbers[i][0] = i + 1
        i += 1
    return users_numbers

def del_position(pos, users_numbers):
    position_name = users_numbers[pos][1]
    response = input(f'Do you want to remove {position_name} from the list? ')
    if response.lower()[:1] == 'y':
        del(users_numbers[pos])
    return users_numbers



def recompile_list(users_numbers):
    final_list = []
    for user in users_numbers:
        final_list.append({'userName': user[1], 'phoneNumber': user[2]})
        print(final_list)
    final_list ={'oncallList':final_list}
    return final_list

def change_number(pos, new_number, users_numbers):
    new_number = format_tel(new_number)
    print(f'Old NUmber: {pretty_format(users_numbers[pos][2])} | New Number: {pretty_format(new_number)}')
    response = input(f'Does this look correct? ')
    if response.lower()[:1] == 'y':
        users_numbers[pos][2] = new_number
    return users_numbers



def format_tel(tel):
    tel = tel.removeprefix("+")
    tel = tel.removeprefix("1")     # remove leading +1 or 1
    tel = re.sub("[ ()-]", '', tel) # remove space, (), -
    #assert(len(tel) == 10)
    tel = f"{tel[:3]}{tel[3:6]}{tel[6:]}"
    return tel

def pretty_format(n):
    n = n.replace('.', '')
    return format(int(n[:-1]), ",").replace(",", "-").replace('.', '') + n[-1]

start_script()
sleep(.1)