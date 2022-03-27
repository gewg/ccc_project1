from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# the master node
if rank == 0:

    msg = 'Hello, world'
    comm.send(msg, dest=1)

# the slave node
else: 
    s = comm.recv()
    print("rank %d: %s" % (rank, s))
else:
    print("rank %d: idle" % (rank))