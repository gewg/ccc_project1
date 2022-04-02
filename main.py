from collections import defaultdict
import mmap
import sys
from mpi4py import MPI
from master import Master
from info_capturer import InfoCapturer
import time

start = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# the twitter data
arg_twitter_file = sys.argv[1]
# the city grid data
arg_city_grid_file = sys.argv[2]
# the two format of grid id
arg_grid_id_map_file = sys.argv[3]
# the language code data
arg_language_code_data_file = sys.argv[4]

# the master node
if rank == 0:

    master = Master()

    '''Get the city grid map'''
    grid_map = master.get_grid_file(arg_city_grid_file)

    '''Get the language map'''
    language_map = master.get_language_code_map(arg_language_code_data_file)

    '''Get the grid matching map'''
    grid_id_match_map = master.get_grid_match_map(arg_grid_id_map_file)

    # the dictinay to store the result
    dict_result = defaultdict(lambda: defaultdict(int))

    # single process if there is only one core
    if (size == 1):
        # create a capturer of information object for current slave process
        info_capturer = InfoCapturer(grid_map)

        '''Get the required information and send to master one by one'''
        # read the file
        with open(arg_twitter_file, "r") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            # skip the first line
            mm.readline()

            # filter the twitter data to match grid
            for line in iter(mm.readline, b""):
                
                # get the information from the current line
                line_info = info_capturer.get_grid_language_info(line)

                # get the grid id and language code from slave
                if (line_info != False):
                    grid_id = line_info[0]
                    language_code = line_info[1]
                    dict_result[grid_id][language_code] += 1

                    # count the total tweets
                    dict_result[grid_id]['total_tweets'] += 1

            mm.close()
        f.close()


    # multiple processes if there are more than one core
    else:
        '''Assign reading's task to slaves'''
        # get the number of twitter data's rows and offset for each line in twitter data
        twitter_info = master.get_twitter_file_info(arg_twitter_file)
        num_twitter_row = twitter_info[0]
        twitter_file_offset = twitter_info[1]

        # get the number of slaves, 'minus one' means 'minus the master node'
        num_slaves = size - 1
        # count how many rows need to be count for each slave, "minus one" means "minus the first line which contains total rows' number"
        num_each_slave = (num_twitter_row - 1) // num_slaves
        # if there is reminder, assign these tasks to the last slave
        num_tasks_reminder = (num_twitter_row - 1) % num_slaves

        # each task's beginning and ending line for each slave
        head_curr_slave = 1
        end_curr_slave = 1
        # send assignment to each slave
        for i in range(1, num_slaves + 1):
            # count the beginning and ending line for each slave
            head_curr_slave = end_curr_slave + 1 # 'plus 1' means 'move the head to the first line for current slave'
            end_curr_slave = head_curr_slave + (num_each_slave - 1) # 'minus 1' because 'the head has been moved to the first line for current slave'

            # plus the reminder if reach the lastest slave
            if (i == num_slaves):
                end_curr_slave += num_tasks_reminder
            
            # send the city grid and offset
            comm.send([grid_map, twitter_file_offset], dest = i, tag = 0)
            # send the assignment
            comm.send([head_curr_slave, end_curr_slave], dest = i, tag = 1)
        

        '''Receive data from slaves and Count the language'''
        # count the number of slaves which complete the task
        num_finish_slave = 0

        # continue receiving until all slaves finish their jobs
        while (num_finish_slave < num_slaves):
            
            # receive the message from slaves
            recv_message = comm.recv(tag = 2)

            if (recv_message == 'Finish'):
                # get the 'finish job' signal from slave
                num_finish_slave += 1
            else:
                # otherwise, get the grid id and language code from slave
                grid_id = recv_message[0]
                language_code = recv_message[1]
                dict_result[grid_id][language_code] += 1

                # count the total tweets
                dict_result[grid_id]['total_tweets'] += 1
        
    '''Print the result in terminal'''
    master.show_result(dict_result, language_map, grid_id_match_map)
    end = time.time()
    print("The execution time is ", time.strftime("%H:%M:%S",time.gmtime(end-start)))
    


# the slave node
else:

    '''Accept message rom master node'''
    # receive city grid and twitter file's offset from master node
    prepare_info = comm.recv(tag = 0)
    grid_map = prepare_info[0]
    twitter_file_offset = prepare_info[1]

    # receive which line need to be read from master node
    assignment = comm.recv(tag = 1)
    assignment_first_line = assignment[0]
    assignment_last_line = assignment[1]
    
    # create a capturer of information object for current slave process
    info_capturer = InfoCapturer(grid_map)

    '''Get the required information and send to master one by one'''
    # read the file
    with open(arg_twitter_file, "r") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        # position head to the assigned task's first line
        mm.seek(twitter_file_offset[assignment_first_line - 1]) # 'minus 1' because 'the first line's subscript in the list is the 0'
        
        # record the line be searched currently
        curr_line = assignment_first_line
        # filter the twitter data to match grid
        for line in iter(mm.readline, b""):
            
            # get the information from the current line
            line_info = info_capturer.get_grid_language_info(line)

            if (line_info != False):
                comm.send(line_info, dest = 0, tag = 2)

            # count how many assignments have been finished
            curr_line += 1

            # break the loop if all the required assignment has been completed
            if (curr_line == assignment_last_line + 1):
                break

        mm.close()
    f.close()

    # send finish message to master node
    comm.send("Finish", dest = 0, tag = 2)