import mmap
import json

class Master:

    def get_grid_file(self, json_file):
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



    def get_twitter_file_info(self, json_file):
        '''
        Get the number of json file's rows and each line's beginning subscript
        
        :param json_file: the file be read
        :return: [int, list], int is the number of file's rows, list is the line's beginning subscript
        '''

        # stores the number of rows
        rows_num = 0
        # marks the position of the first byte for each line in twitter data
        offsets = [0]

        # search the first line in json file to get the row's number
        with open(json_file, "r") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            '''
            This part counts the total rows according to the attribute 'total_row', which is not same as the actual number

            # search the starting subscript and ending subscript of 'number' from the bytes
            # eg.: b'{"total_rows":5000, "rows":[\r\n'
            head = mm.find(b':') + 1
            tail = mm.find(b',')
            # get the number of file's row
            rows_num = int(mm[head : tail])
            '''

            # count the offset 
            for line in iter(mm.readline, b''):

                # add the beginning byte's postion of current line in the file
                offsets.append(mm.tell())

                # count the row's number
                rows_num += 1

            mm.close()
        f.close()
        
        return [rows_num, offsets]

    def get_language_code_map(self, json_file):
        '''
        Get the map to connect language with language code

        :param json_file: contains the relation between language and langauge code
        :return: dictionary, 'key as language code' and 'value as language's name'
        '''

        # read the json file
        f = open(json_file,"r")
        languages  = json.load(f)

        # stores the grid dictionary
        language_map = {}
        
        # search the grid id and grid coordinates
        for curr_language in languages["values"]:
            language_map[curr_language["code"]] = curr_language["name"]


        return language_map



