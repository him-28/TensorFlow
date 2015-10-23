#auto_deploy.sh
#!/bin/bash

##############################
# 传参说明：
# -p 必须参数，部署到哪个目录下，可传相对、绝对路径
# -c 可选，指定配置文件路径，替换conf/config.yml
# -a 可选，指定audit配置文件路径，替换conf/audit_config.yml
# -l 可选，指定log配置文件的路径，替换util/logger.conf
#
# ex. sh auto_deploy -p /home/test/etl
##############################

root_path=""
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

if [ ! "$conf_file" == "" -a ! -f "$conf_file" ];then
    echo "the conf file did not exists:$conf_file"
    exit 1
fi
if [ ! "$audit_conf_file" == "" -a ! -f "$audit_conf_file" ];then
    echo "the audit conf file did not exists:$audit_conf_file"
    exit 1
fi
if [ ! "$log_conf_file" == "" -a ! -f "$log_conf_file" ];then
    echo "the log conf file did not exists:$log_conf_file"
    exit 1
fi

if [ "$root_path" == "" ];then
    echo "the arg -p must be set"
    exit 1
fi


[[ $root_path != *"/" ]] && root_path="$root_path/"


echo "deploy to dir:$root_path"

git init ${root_path}
cd ${root_path}
git pull git@git.hunantv.com:Data-S/amble.git

dep_conf_path=${root_path}"etl/conf/config.yml"
if [ ! "$conf_file" == "" ];then
    echo "copy conf file:$conf_file to:"${dep_conf_path}
    rm -f ${dep_conf_path}
    cp -f ${conf_file} ${dep_conf_path}
fi
dep_audit_conf_path=${root_path}"etl/conf/audit_config.yml"
if [ ! "$audit_conf_file" == "" ];then
    echo "copy audit conf file:$audit_conf_file to:"${dep_audit_conf_path}
    rm -f ${dep_audit_conf_path}
    cp -f ${audit_conf_file} ${dep_audit_conf_path}
fi
dep_log_conf_file=${root_path}"etl/util/logger.conf"
if [ ! "$log_conf_file" == "" ];then
    echo "the log conf file:$log_conf_file to:"${dep_log_conf_file}
    rm -f ${dep_log_conf_file}
    cp -f ${log_conf_file} ${dep_log_conf_file}
fi

echo "deploy completed"
