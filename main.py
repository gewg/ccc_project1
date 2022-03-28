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

    '''Assign reading's task to slaves'''
    # get the number of twitter data's rows
    num_twitter_row = master.get_twitter_file_rows("data/smallTwitter.json")

    # get the number of slaves, 'minus one' means 'minus the master node'
    num_slaves = size - 1
    # count how many rows need to be count for each slave, "minus one" means "minus the first line which contains total rows' number"
    num_each_slave = (num_twitter_row - 1) // num_slaves
    # if there is reminder, assign these tasks to the last slave
    num_tasks_reminder = (num_twitter_row - 1) % num_slaves

    # the task's beginning and ending for current slave
    head_curr_slave = 1
    end_curr_slave = 1
    # send assignment to each slave
    for i in range(1, num_slaves + 1):
        # count the beginning and ending
        head_curr_slave = end_curr_slave + 1 # 'plus 1' means 'move the head to the first line for current slave'
        end_curr_slave = head_curr_slave + (num_each_slave - 1) # 'minus 1' because 'the head has been moved to the first line for current slave'

        # plus the reminder if reach the lastest slave
        if (i == num_slaves):
            end_curr_slave += num_tasks_reminder
        
        # send city grid
        comm.send(grid_map)
        # send task
        comm.send([head_curr_slave, end_curr_slave], dest = i)
    

    ''''''
    '''Count the number of language'''




# the slave node
else:

    '''Accept the task from master'''
    # receive the message from master node
    message_master = comm.recv()

    slave = Slave()

    '''Get the '''




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