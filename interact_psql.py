import pexpect
import re
import time
import readline

class FredsQL_state:
    def __init__(self):
        self.current_database = None
        self.username = None
        self.psql_process = None

def start_psql():
    fredsql_state = FredsQL_state()

    # Start psql as a child process
    process = pexpect.spawn('psql', encoding='utf-8')

    fredsql_state.psql_process = process

    # Wait for psql to start, and output initial prompt, we don't print it though
    wait_for_execution(process, '=#')

    # We need the current database and user to be able to provide a nice prompt for the user of fredsql
    extract_and_set_database_name_and_user(fredsql_state)

    # print("Returning fredstate:")
    # print(fredsql_state.current_database)
    # print(fredsql_state.username)

    # Disable the pager because my program can't handle it

    run_psql_command(fredsql_state, '\pset pager off')

    return fredsql_state

def extract_and_set_database_name_and_user(fredsql):
    data = run_psql_command(fredsql, '\c')

    # Extract the database that we are on
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
            output = wait_for_execution(fsql_state.psql_process, '=#')
        else:
            output = wait_for_execution(fsql_state.psql_process, fsql_state.current_database + '=#')

    except pexpect.EOF:
        # Handle the end of the psql process
        print("PSQL process terminated.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return output

if __name__ == "__main__":
    # Start psql process
    fredsql_state = start_psql()

    while True:
        # Get user input
        user_input = input(fredsql_state.username + '@' + fredsql_state.current_database + '-> ')

        # Check for exit command
        if user_input.lower().strip() == 'exit':
            fredsql_state.psql_process.terminate()
            break

        # Check for \c to change database in case we need to adjust the end_of_output_pattern
        # or we can't know when a query has returned and it will just get stuck in the expect-call
        if user_input.lower().strip().startswith('\c'):
            # Must null the current db as now the end of output will look different and we can only look at =#
            fredsql_state.current_database = None
            print(run_psql_command(fredsql_state, user_input))
            extract_and_set_database_name_and_user(fredsql_state)

            continue

        print(run_psql_command(fredsql_state, user_input))
