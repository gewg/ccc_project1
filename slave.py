from mpi4py import MPI
import mmap


class Slave:

    def __init__(self, grid_map):
        self.grid_map = grid_map
    

    def filter_twitter_data():
        '''
        Remove the data which is not in the grid
        '''
        inRange_data = []
        for data in filterd_data:
            for key,value in gridDict.items():
                if coordsInGrid(data["doc"]["coordinates"]["coordinates"],value):
                    inRange_data.append(data)
                    break
    

    def read_twitter_json_file(json_file, start, end):
        '''
        Read the large twitter json file 
        
        :param json_file: the file be read
        :start: the first line to read
        :end: the lastest line to read

        :no return
        
        '''

        # read the file
        with open(json_file, "r") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            # filter the twitter data to match grid
            for line in iter(mm.readline, b""):

                