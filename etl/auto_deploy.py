#!/usr/bin/env python
# encoding: utf-8
'''
etl.auto_deploy -- etl auto deploy script

Use to deploy etl codes to the servers

It defines task_deploy to run shell scripts in hosts.

@author:     dico
@copyright:  2015 http://www.hunantv.com All rights reserved.
@contact:    dingzheng@imgo.tv
'''

import sys
import os

from optparse import OptionParser

from fabric.api import run,env,execute,roles

__all__ = []
__version__ = 0.2
__date__ = '2015-10-23'
__updated__ = '2015-10-26'

DEBUG = 0 # 调试标志

#########################################################################################
# Usage: auto_deploy.py [options]
#
# Options:
#  --version             show program's version number and exit
#  -h, --help            show this help message and exit
#  -s Host, --server=Host
#                        set deploy host,should be a IP address or
#                        domain name [default: 127.0.0.1]
#  -u User, --user=User  set user in the host [default: dingzheng]
#  -p Port, --port=Port  set the prot of the ssh [default: 22]
#  -d Directory, --dir=Directory
#                        set the root directory witch the code will
#                        deploy in [default: ~/etl/]
#########################################################################################
# EX. python auto_deploy.py -s 10.100.5.82 -u dingzheng -p 22 -d /home/dingzheng/etl_test
#########################################################################################

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
def task_deploy(root_dir):
    '''发布shell脚本'''
    shell_script = '''
root_path="''' + root_dir + '''"
if [ "$root_path" == "" ];then
    echo "root_path must be set"
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
    sys.exit(main())
