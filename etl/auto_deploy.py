#!/usr/bin/env python
# encoding: utf-8
'''
etl.auto_deploy -- etl auto deploy script

Use to deploy etl codes to the servers

It defines classes_and_methods

@author:     dico
@copyright:  2015 http://www.hunantv.com All rights reserved.
@contact:    dingzheng@imgo.tv
'''

import sys
import os

from optparse import OptionParser

from fabric.api import *

__all__ = []
__version__ = 0.1
__date__ = '2015-10-23'
__updated__ = '2015-10-23'

DEBUG = 0
TESTRUN = 0
PROFILE = 0

def main(argv=None):
    '''Command line options.'''

    program_name = os.path.basename(sys.argv[0])
    program_version = "v0.1"
    program_build_date = "%s" % __updated__

    program_version_string = '%%prog %s (%s)' % (program_version, program_build_date)
    program_longdesc = ''''''  # optional - give further explanation about what the program does
    program_license = "Copyright 2006-2015 hunantv.com Corporation,All Rights Reserved\
                        \nhttp://www.hunantv.com/"

    if argv is None:
        argv = sys.argv[1:]
    try:
        # setup option parser
        parser = OptionParser(version=program_version_string, \
                              epilog=program_longdesc, description=program_license)
        parser.add_option("-s", "--server", dest="host", metavar="Host", \
                          help="set deploy host,should be a IP address or\
                           domain name [default: %default]")
        parser.add_option("-u", "--user", dest="user", \
                          help="set user in the host [default: %default]", metavar="User")
        parser.add_option("-p", "--port", dest="port", \
                          help="set the prot of the ssh [default: %default]", metavar="Port")
        parser.add_option("-d", "--dir", dest="directory", metavar="Directory", \
                          help="set the root directory witch the code will\
                           deploy in [default: %default]")

        # set defaults
        parser.set_defaults(host="127.0.0.1", user="dingzheng", port=22, directory="~/etl/")

        # process options
        (opts, args) = parser.parse_args(argv)
        print "process options:%s" % str(args)
        print "running deploy by : host = %s, user = %s, port = %s, dir=%s"\
               % (opts.host, opts.user, opts.port, opts.directory)

        env.roledefs = {
            'theserver': ['%s@%s:%s' % (opts.user, opts.host, opts.port)]
            }

        execute(task_deploy, opts.directory)

    except Exception, exc:
        import traceback
        traceback.format_exc()
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(exc) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

@roles("theserver")
def task_deploy(dir):
    '''发布shell脚本'''
    shell_script = '''
    
root_path="''' + dir + '''"
conf_file=""
audit_conf_file=""
log_conf_file=""

while getopts "p:c:a:l:" arg
do
    case $arg in
        p)
            echo "use deploy dir:$OPTARG"
            root_path="$OPTARG"
            ;;
        c)
            echo "use conf file:$OPTARG"
            conf_file="$OPTARG"
            ;;
        a)
            echo "use audit conf file:$OPTARG"
            audit_conf_file="$OPTARG"
            ;;
        l)
            echo "use log conf file:$OPTARG"
            log_conf_file="$OPTARG"
            ;;
        ?)
            echo "unknow argument"
            exit 1
            ;;
    esac
done


if [ "$root_path" == "" ];then
    echo "the arg -p must be set"
    exit 1
fi


[[ $root_path != *"/" ]] && root_path="$root_path/"


echo "deploy to dir:$root_path"

git init ${root_path}
cd ${root_path}
git pull git@git.hunantv.com:Data-S/amble.git

echo "deploy completed"

    '''
    run(shell_script)



if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-h")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        PROFILE_FILENAME = 'etl.auto_deploy_profile.txt'
        cProfile.run('main()', PROFILE_FILENAME)
        STATSFILE = open("profile_stats.txt", "wb")
        P_STATS = pstats.Stats(PROFILE_FILENAME, stream=STATSFILE)
        STATS = P_STATS.strip_dirs().sort_stats('cumulative')
        STATS.print_stats()
        STATSFILE.close()
        sys.exit(0)
    sys.exit(main())
