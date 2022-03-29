import mmap
import json


class Slave:

    def __init__(self, grid_map):
        self.grid_map = grid_map


    def get_grid_info(self, coordinate):
        '''
        Get the useful data from twitter file's each line

        :param coordinate: a tuple with coordinate
        :return boolean or int: 'false if coordinate does not match the city grid' or 'grid id if matches'
        '''
        # check whether current line's coordinate exists
        potential_grid = []

        for key, value in self.grid_map.items():
            lat1 = value[0][0]
            lat2 = value[2][0]
            lon1 = value[2][-1]
            lon2 = value[0][-1]
            if (lat1 <= coordinate[0] and coordinate[0] <= lat2):
                if (lon1 <= coordinate[1] and coordinate[1] <= lon2):
                    potential_grid.append(key)
        
        if len(potential_grid)==0:
            return False
        if len(potential_grid)==2:
            if potential_grid[-1] - potential_grid[0] == 4:
                return potential_grid[-1]
            else:
                return potential_grid[0]
        

    
    def get_grid_language_info(self, line):
        '''
        Capture grid id and language from twitter file's line

        :param line: row from twitter file
        :return boolean / [int, string]: 'false if the line is useless' or '[grid id, language code]'
        '''

        # convert current line from byte to string format
        line = line.decode('utf8')
        # remove useless element and convert current line to json format
        line = line.replace('"location":"sydney"}},', '"location":"sydney"}}')
        line = line.replace('"location":"sydney"}}]}', '"location":"sydney"}}')

        # convert current line from string to json format 
        # this exception capture is just for a varied single line from bigTwitter.json: ']}', this line is not any row's part
        # other several data files all runs well without exception capture
        try:
            line = json.loads(line) 
        except:
            #print(line)
            return False

        # get the coordinate and language code from the current line
        coordinates_info = line["doc"]["coordinates"]
        language_code = line["doc"]["lang"]

         # check the coordinate's belonging
        if (coordinates_info != None):
            coordinate = coordinates_info["coordinates"]
            
            # check the gird's belonging
            grid_id = self.get_grid_info(coordinate)
            if (grid_id != False and language_code != 'und'):
                return [grid_id, language_code]
        
        return False
            



                

                