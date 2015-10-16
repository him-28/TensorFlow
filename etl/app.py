import sys
if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter
from etl.logic0.etl_transform import hour_etl
from etl.audit import *

def run_cli(arguments):
    if arguments == '0':
        print arguments
    elif arguments == '1':
        pass

if __name__ == '__main__':
#     run_cli(sys.argv[1:])
    hour_etl('20150923','11','hour','new')
