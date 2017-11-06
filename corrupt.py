
import sys

filename = sys.argv[1]
corrupt = int(sys.argv[2])


with open(filename, 'rb') as in_file:
    _bytes = in_file.read()[:-corrupt]
    
#_bytes = _bytes[:corrupt] + '0' + _bytes[corrupt+1:]
    
with open(filename, 'wb') as out_file:
    out_file.write(_bytes)        