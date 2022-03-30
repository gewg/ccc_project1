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


    def get_grid_match_map(self, json_file):
        '''
        Get the map to connect two formats of grid id

        :param json_file: contains the relation between the two formats
        :return: dictionary, 'key as grid id' and 'value as serial number' eg. {'9':'A1'}
        '''

        # read the json file
        f = open(json_file,"r")
        grids  = json.load(f)

        # stores the grid dictionary
        grid_id_match_map = {}
        
        # search the grid id and grid coordinates
        for curr_grid in grids["values"]:
            grid_id_match_map[curr_grid["grid_id"]] = curr_grid["serial_number"]


        return grid_id_match_map


    def show_result(self, dict_result, language_map, grid_id_match_map):
        '''
        Print the result in terminal

        :param dict_result: the dictionary with 'key as gird id' and 'value as another dictionary'
        :param dict_result: the another dictionary with 'key as language code' and 'value corresponding user's amount of key'
        '''

        # show the title
        print(f"Cell   #Total Tweets   #Number of Languages Used         #Top 10 Languages & #Tweets")

        # sort the dictionary by id
        dict_result = {k: v for k, v in sorted(dict_result.items(), key=lambda item: grid_id_match_map[item[0]])}

        # show the grid's one by one
        for curr_grid_id in dict_result.keys():
            
            # get the current grid's dictionary
            curr_grid_dict = dict_result[curr_grid_id]

            # get info
            total_tweets = curr_grid_dict['total_tweets']
            total_language_be_used = len(curr_grid_dict.keys()) - 1 # minus 1 means minus the 'total_tweets' attribute

            # sort dictionary to get the top 10 language
            curr_grid_dict = {k: v for k, v in sorted(curr_grid_dict.items(), key=lambda item: item[1], reverse=True)}
            
            # search the top 10 language
            top_10_string = ""
            top_10_count = 0
            for curr_language_code in curr_grid_dict.keys():
                
                # skip the largest element which is the total tweets
                if (curr_language_code == 'total_tweets'):
                    continue

                # combine the string
                top_10_string += f"{language_map[curr_language_code]}-{curr_grid_dict[curr_language_code]}, "
                top_10_count +=1

                # stop searching when loop reaches 10 language
                if (top_10_count == 11):
                    break
            
            # delete the lastest comma and blank
            top_10_string = top_10_string[:-2]
            # get the grid id's another format
            curr_grid_id = grid_id_match_map[curr_grid_id]
            # show the current cell's info
            print('{0:<8} {1:<21} {2:<25} {3:}'.format(curr_grid_id, total_tweets, total_language_be_used, "("+top_10_string+")"))




