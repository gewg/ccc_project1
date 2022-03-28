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



        return 1


    def convert_byte_to_json(self, byte_line):
        '''
        Accept byte string from twitter file, transfer it to json format

        :param byte_line: the byte string
        :return: json string
        '''
        # convert current line from byte to string format
        byte_line = byte_line.decode('utf8')
        # remove useless element and convert current line to json format
        byte_line = byte_line.replace('"location":"sydney"}},', '"location":"sydney"}}')
        byte_line = byte_line.replace('"location":"sydney"}}]}', '"location":"sydney"}}')
        # convert current line from string to json format
        byte_line = json.loads(byte_line) 
 
        return byte_line




                

                