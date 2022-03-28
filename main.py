import mmap
from ast import literal_eval
from mpi4py import MPI
from master import Master
from slave import Slave

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# the master node
if rank == 0:

    master = Master()

    '''Get the city grid map'''
    grid_map = master.get_grid_file("data/sydGrid.json")

    '''Get the language map'''
    language_map = master.get_language_code_map("data/language_code.json")

    '''Assign reading's task to slaves'''
    # get the number of twitter data's rows and offset for each line in twitter data
    twitter_info = master.get_twitter_file_info("data/tinyTwitter.json")
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

    # the stack to receive row's information from slave

    # continue loop before 
    while (num_finish_slave < num_slaves):

        # get the finish mark from slave which completes its task
        if (comm.recv(tag = 3)):
            num_finish_slave += 1

        curr_row = comm.recv(tag = 2)

        print(num_finish_slave)
    
    # stop the slaves




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
    
    # create a slave object for current slave process
    slave = Slave(grid_map)

    '''Get the required information and send to master one by one'''
    # read the file
    json_file = "data/smallTwitter.json"
    with open(json_file, "r") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        # position head to the assigned task's first line
        mm.seek(twitter_file_offset[assignment_first_line - 1]) # 'minus 1' because 'the first line's subscript in the list is the 0'
        
        # record the line be searched currently
        curr_line = assignment_first_line
        # filter the twitter data to match grid
        for line in iter(mm.readline, b""):
            
            # convert the current line from byte string to json string
            line = slave.convert_byte_to_json(line)

            # get the coordinate and language code from the current line
            coordinates_info = line["doc"]["coordinates"]
            langauge_code = line["doc"]["lang"]

            # check the coordinate's belonging
            if (coordinates_info != None):
                coordinate = coordinates_info["coordinates"]
                
                # get the corresponding grid information of the coordinate
                grid_id = slave.get_grid_info(coordinate)
                if (grid_id != False):
                    
                    # send the grid id and language code
                    comm.send([grid_id, langauge_code], dest = 0, tag = 2)

            curr_line += 1
            # break the loop if the required assignment has been finished
            if (curr_line == assignment_last_line + 1):
                break

        mm.close()
    f.close()

    # send finish message to master node
    comm.send("Finish", dest = 0, tag = 3)


# # the master node
# if rank == 0:

#     n=0
#     while (n <= 5):
#         s = comm.recv()
#         print(s)
#         n += 1

# # the slave node
# elif rank: 
#     for i in range(0, 2):
#         comm.send(rank, dest=0)