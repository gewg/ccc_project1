import mmap

class Slave:
    
    def __init__(self) -> None:
        pass

    def read_json_file(json_file):
        '''
        Read the large json file 
        
        '''

        # read the file
        with open(json_file, "r+b") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            for line in iter(mm.readline, b""):
                print(line)
                print("/n")