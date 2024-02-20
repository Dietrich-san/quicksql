import pexpect
import re
import time
import readline
import sys
from quicksql import convert_to_sql
from getpass import getpass

# This may differ and should probably be put in a config... or auto detected
psql_prompt_symbol = None

class FredsQL_state:
    def __init__(self):
        self.current_database = None
        self.username = None
        self.psql_process = None

    def to_string(self):
     return "Current database: " + self.current_database + "\nUsername: " + self.username + "\nSymbol: " + psql_prompt_symbol

def start_psql(args):
    global psql_prompt_symbol

    args = args.strip()
    fredsql_state = FredsQL_state()

    psql_start_string = 'psql'

    if len(args) > 0:
        psql_start_string = psql_start_string + ' ' + args

    # Start psql as a child process
    fredsql_state.psql_process = pexpect.spawn(psql_start_string, encoding='utf-8')

    time.sleep(0.1)

    index_of_what_was_matched = fredsql_state.psql_process.expect(['Password for user [A-z]+:', '=#', '=>'])

    if index_of_what_was_matched == 0:
        # password prompt
        # password = input('Enter your password: ')

        password = getpass()

        fredsql_state.psql_process.sendline(password)

        index_of_what_was_matched = fredsql_state.psql_process.expect(['=#', '=>'])

        if index_of_what_was_matched == 0:
            # first prompt symbol
            print('First symbol set (1)')
            psql_prompt_symbol = '=#'
        elif index_of_what_was_matched == 1:
            # second prompt symbol
            print('Second symbol set (1)')
            psql_prompt_symbol = '=>'
        else:
            raise Exception('Impossible!')

    elif index_of_what_was_matched == 1:
        # first prompt symbol
        print('First symbol set (2)')
        psql_prompt_symbol = '=#'
    elif index_of_what_was_matched == 2:
        # second prompt symbol
        print('Second symbol set (2)')
        psql_prompt_symbol = '=>'
    else:
        raise Exception('Impossible!')

    if psql_prompt_symbol == None:
        raise Exception('I dont know what the symbol for end of output is!')

    #print(fredsql_state.psql_process.before)

    # Wait for psql to start, and output initial prompt, we don't print it though
    #wait_for_execution(fredsql_state.psql_process, psql_prompt_symbol)

    # We need the current database and user to be able to provide a nice prompt for the user of fredsql
    extract_and_set_database_name_and_user(fredsql_state)

    # Disable the pager because my program can't handle it

    run_psql_command(fredsql_state, '\pset pager off')

    return fredsql_state

def extract_and_set_database_name_and_user(fredsql):
    data = run_psql_command(fredsql, '\c')

    # Extract the database that we are on
    match = re.search('rejects connection for host "[0-9.]+", user "(.+)", database "(.+)"', data)
    if match != None:
        fredsql.username = match[1]
        fredsql.current_database = match[2]
    else: # match is None
        match = re.search('You are now connected to database \"(.+)" as user "(.+)"', data)
        if match == None:
            raise Exception('Unexpected output!')

        fredsql.current_database = match[1]
        fredsql.username = match[2]

    if fredsql.current_database == None or fredsql.current_database.strip() == '':
        raise Exception('Failed to set database!')

    if fredsql.username == None or fredsql.username.strip() == '':
        raise Exception('Failed to set database user!')

def wait_for_execution(psql_process, end_of_output_pattern):

    # We have to sleep here at first, to not accidently think that the middle of the output is the end.
    # That should only happen if the output contains exactly <database_name>=#, which is
    # unlikely. But could still happen, and would appear very strange for the user.
    # The user could basically never query that content just because of that.
    # And if the database is something like "a" it could happen more often which is unacceptable.
    time.sleep(0.03)

    # Wait for the command to finish
    psql_process.expect_exact(pattern_list = [end_of_output_pattern], searchwindowsize=50)

    return psql_process.before

def run_psql_command(fsql_state, command):

    if fsql_state.psql_process == None:
        raise Exception("Cannot run command in psql, no psql process!")

    if (fsql_state.current_database == None or len(fsql_state.current_database.strip()) == 0) and not (command.startswith('\c')):
        raise Exception("No data base defined")

    try:
        # Send the command to the psql process
        fsql_state.psql_process.sendline(command)
        if fsql_state.current_database == None:
            # if we don't know the database
            output = wait_for_execution(fsql_state.psql_process, psql_prompt_symbol)
        else:
            output = wait_for_execution(fsql_state.psql_process, fsql_state.current_database + psql_prompt_symbol)

    except pexpect.EOF:
        # Handle the end of the psql process
        print("PSQL process terminated.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return output

if __name__ == "__main__":

    arg_string = ''
    # skip the name of the program
    sys.argv = sys.argv[1:]
    for arg_part in sys.argv:
        arg_string = arg_string + ' ' + arg_part

    print(arg_string)

    # Start psql process
    fredsql_state = start_psql(arg_string)

    while True:
        # Get user input
        user_input = input(fredsql_state.username + '@' + fredsql_state.current_database + '-> ')

        # Check for exit command
        if user_input.lower().strip() == 'exit' or user_input.lower().strip() == 'exit;':
            fredsql_state.psql_process.terminate()
            break

        # Check for qsql commands
        if user_input.lower().strip() == '\q_state':
            print("State of qsql:\n" + fredsql_state.to_string())
            continue

        # Check for \c to change database in case we need to adjust the end_of_output_pattern
        # or we can't know when a query has returned and it will just get stuck in the expect-call
        if user_input.lower().strip().startswith('\c'):
            # Must null the current db as now the end of output will look different and we can only look at =#
            fredsql_state.current_database = None
            print(run_psql_command(fredsql_state, user_input))
            extract_and_set_database_name_and_user(fredsql_state)

            continue

        # Check for quicksql query
        if user_input[0] == '!':
            try:
                user_input = convert_to_sql(False, user_input)

                print("Quicksql -> sql: " + user_input)
            except BaseException as error:
                print("Could not translate supposed quicksql query: " + user_input + ", to sql.\nError raised from quicksql translator:\n\n {}".format(error))
                continue

        print(run_psql_command(fredsql_state, user_input))
