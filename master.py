import mmap
import json

class Master:

    def __init__(self) -> None:
        pass


    def get_grid_file(json_file):
        '''
        Get the city grid from json file

        :return: dictionry, 'grid id as key' and 'a list with grid coordinates as value'
        '''

        # read the json file
        f = open(json_file,"r")
        grid_data  = json.load(f)

        # stores the grid dictionary
        grid_dict = {}
        
        # search the grid id and grid coordinates
        for curr_grid_feature in grid_data["features"]:
            grid_dict[curr_grid_feature["properties"]["id"]] = curr_grid_feature["geometry"]["coordinates"][0]

        grid_dict = dict(sorted(grid_dict.items()))

        return grid_dict



    def get_twitter_file_rows(json_file):
        '''
        Get the number of json file's rows
        
        :param json_file: the file be read
        :return: int, the number of file's rows
        '''

        # stores the number of rows
        rows_num = 0

        # search the first line in json file to get the row's number
        with open(json_file, "r") as f:

            # open the file
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            # search the starting subscript and ending subscript of 'number' from the bytes
            # eg.: b'{"total_rows":5000, "rows":[\r\n'
            head = mm.find(b':') + 1
            tail = mm.find(b',')
            # get the number of file's row
            rows_num = int(mm[head : tail])
            

            # close the file
            mm.close()


        return rows_num
