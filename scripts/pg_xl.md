pg_xl 部署
=======

部署机器：10.100.5.80

部署环境：centos-x64

部署软件版本： postgres-xl-v9.2

部署规划：一个gtm主节点 四个gtm-proxy节点 四个coordinator节点 四个datanode节点

1.两台机器添加hosts
<pre><code>
10.100.5.80	adpg0
10.100.5.81	adpg1
</code></pre>

2.两台机器安装pg_xl:
<pre><code>
cd /home/dongjie
tar -zxvf postgres-xl-v9.2-src.tar.gz
cd postgres-xl
./configure --prefix=/home/dongjie/pgxl9.2 --with-pgport=11921  
gmake world
gmake install-world
</code></pre>
可以根据configure的提示安装所需要依赖包

3.创建相关目录
<pre><code>
80机器：
cd /home/dongjie
mkdir datanode12921 datanode12922 cood11921 cood11922 gtm13921 gtmprox14921 gtmprox14922
81机器：
cd /home/dongjie
mkdir datanode12923 datanode12924 cood11923 cood11924 gtmprox14923 gtmprox14924
</code></pre>

4.两台机器配置.bash_profile文件
<pre><code>
PATH=$PATH:$HOME/bin
export PGDATA=/postgres/pgxl/
export LANG=en_US.utf8
export PGHOME=/home/dongjie/pgxl9.2
export LD_LIBRARY_PATH=$PGHOME/lib:/lib64:/usr/lib64:/usr/local/lib64:/lib:/usr/lib:/usr/local/lib:$LD_LIBRARY_PATH
export DATE=`date +"%Y%m%d%H%M"`
export PATH=$PGHOME/bin:$PATH:.
export MANPATH=$PGHOME/share/man:$MANPATH
export PGHOST=$PGDATA
export PGPORT=11921
export PGUSER=postgres
export PGDATABASE=postgres

export PATH
</code></pre>

5.80机器配置gtm节点
<pre><code>
initgtm -Z gtm -D /home/dongjie/gtm13921/
vim /home/dongjie/gtm13921/gtm.conf
nodename = 'gtm13921'                   # Specifies the node name.
listen_addresses = '*'                  # Listen addresses of this GTM.
port = 13921                            # Port number of this GTM.
startup = ACT                           # Start mode. ACT/STANDBY.
keepalives_idle = 60                    # Keepalives_idle parameter.
keepalives_interval = 10                # Keepalives_interval parameter.
keepalives_count = 10                   # Keepalives_count internal parameter.
log_file = 'gtm.log'                    # Log file name
log_min_messages = WARNING              # log_min_messages.  Default WARNING.
</code></pre>

6.配置gtm-proxy节点：
<pre><code>
80机器：
initgtm -Z gtm_proxy -D /home/dongjie/gtmprox14921/
initgtm -Z gtm_proxy -D /home/dongjie/gtmprox14922/

vim /home/dongjie/gtmprox14921/gtm_proxy.conf

nodename = 'gtmprox14921'                                   # Specifies the node name.
listen_addresses = '*'                    # Listen addresses of this GTM.
port = 14921                                    # Port number of this GTM.
worker_threads = 1                              # Number of the worker thread of this
gtm_host = 'adpg0'                                       # Listen address of the active GTM.
gtm_port = 13921                                        # Port number of the active GTM.
gtm_connect_retry_interval = 5  # How long (in secs) to wait until the next
keepalives_idle = 60                    # Keepalives_idle parameter.
keepalives_interval = 10                # Keepalives_interval parameter.
keepalives_count = 10                   # Keepalives_count internal parameter.
log_file = 'gtm_proxy.log'              # Log file name
log_min_messages = WARNING              # log_min_messages.  Default WARNING.

vim /home/dongjie/gtmprox14922/gtm_proxy.conf

nodename = 'gtmprox14922'                                   # Specifies the node name.
listen_addresses = '*'                    # Listen addresses of this GTM.
port = 14922                                    # Port number of this GTM.
worker_threads = 1                              # Number of the worker thread of this
gtm_host = 'adpg0'                                       # Listen address of the active GTM.
gtm_port = 13921                                        # Port number of the active GTM.
gtm_connect_retry_interval = 5  # How long (in secs) to wait until the next
keepalives_idle = 60                    # Keepalives_idle parameter.
keepalives_interval = 10                # Keepalives_interval parameter.
keepalives_count = 10                   # Keepalives_count internal parameter.
log_file = 'gtm_proxy.log'              # Log file name
log_min_messages = WARNING              # log_min_messages.  Default WARNING.

81机器：
initgtm -Z gtm_proxy -D /home/dongjie/gtmprox14923/
initgtm -Z gtm_proxy -D /home/dongjie/gtmprox14924/

vim /home/dongjie/gtmprox14923/gtm_proxy.conf

nodename = 'gtmprox14923'                                   # Specifies the node name.
listen_addresses = '*'                    # Listen addresses of this GTM.
port = 14923                                    # Port number of this GTM.
worker_threads = 1                              # Number of the worker thread of this
gtm_host = 'adpg0'                                       # Listen address of the active GTM.
gtm_port = 13921                                        # Port number of the active GTM.
gtm_connect_retry_interval = 5  # How long (in secs) to wait until the next
keepalives_idle = 60                    # Keepalives_idle parameter.
keepalives_interval = 10                # Keepalives_interval parameter.
keepalives_count = 10                   # Keepalives_count internal parameter.
log_file = 'gtm_proxy.log'              # Log file name
log_min_messages = WARNING              # log_min_messages.  Default WARNING.

vim /home/dongjie/gtmprox14924/gtm_proxy.conf

nodename = 'gtmprox14924'                                   # Specifies the node name.
listen_addresses = '*'                    # Listen addresses of this GTM.
port = 14924                                    # Port number of this GTM.
worker_threads = 1                              # Number of the worker thread of this
gtm_host = 'adpg0'                                       # Listen address of the active GTM.
gtm_port = 13921                                        # Port number of the active GTM.
gtm_connect_retry_interval = 5  # How long (in secs) to wait until the next
keepalives_idle = 60                    # Keepalives_idle parameter.
keepalives_interval = 10                # Keepalives_interval parameter.
keepalives_count = 10                   # Keepalives_count internal parameter.
log_file = 'gtm_proxy.log'              # Log file name
log_min_messages = WARNING              # log_min_messages.  Default WARNING.
</code></pre>

7.配置datanode节点
<pre><code>
80机器：
initdb -D /home/dongjie/datanode12921/ -E UTF8 --locale=C -U postgres -W --nodename datanode12921
initdb -D /home/dongjie/datanode12922/ -E UTF8 --locale=C -U postgres -W --nodename datanode12922

vim /home/dongjie/datanode12921/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 12921                            # (change requires restart)
gtm_host = 'adpg0'                  # Host name or address of GTM
gtm_port = 14921                        # Port of GTM
pgxc_node_name = 'datanode12921'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6667

vim /home/dongjie/datanode12922/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 12922                            # (change requires restart)
gtm_host = 'adpg0'                  # Host name or address of GTM
gtm_port = 14922                        # Port of GTM
pgxc_node_name = 'datanode12922'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6668

81机器：
initdb -D /home/dongjie/datanode12923/ -E UTF8 --locale=C -U postgres -W --nodename datanode12923
initdb -D /home/dongjie/datanode12924/ -E UTF8 --locale=C -U postgres -W --nodename datanode12924

vim /home/dongjie/datanode12923/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 12923                            # (change requires restart)
gtm_host = 'adpg1'                  # Host name or address of GTM
gtm_port = 14923                        # Port of GTM
pgxc_node_name = 'datanode12923'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6667

vim /home/dongjie/datanode12924/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 12924                            # (change requires restart)
gtm_host = 'adpg1'                  # Host name or address of GTM
gtm_port = 14924                        # Port of GTM
pgxc_node_name = 'datanode12924'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6668
</code></pre>

8.配置coordinator节点
<pre><code>
80机器：
initdb -D /home/dongjie/coord11921 -E UTF8 --locale=C -U postgres -W --nodename coord11921
initdb -D /home/dongjie/coord11922 -E UTF8 --locale=C -U postgres -W --nodename coord11922

vim /home/dongjie/coord11921/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 11921                            # (change requires restart)
gtm_host = 'adpg0'                  # Host name or address of GTM
gtm_port = 14921                        # Port of GTM
pgxc_node_name = 'coord11921'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6669
default_transaction_isolation = 'read committed'
default_transaction_read_only = off

vim /home/dongjie/coord11922/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 11922                            # (change requires restart)
gtm_host = 'adpg0'                  # Host name or address of GTM
gtm_port = 14922                        # Port of GTM
pgxc_node_name = 'coord11922'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6670
default_transaction_isolation = 'read committed'
default_transaction_read_only = off

81机器：
initdb -D /home/dongjie/coord11923 -E UTF8 --locale=C -U postgres -W --nodename coord11923
initdb -D /home/dongjie/coord11924 -E UTF8 --locale=C -U postgres -W --nodename coord11924

vim /home/dongjie/coord11923/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 11923                            # (change requires restart)
gtm_host = 'adpg1'                  # Host name or address of GTM
gtm_port = 14923                        # Port of GTM
pgxc_node_name = 'coord11923'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6669
default_transaction_isolation = 'read committed'
default_transaction_read_only = off

vim /home/dongjie/coord11924/postgresql.conf

listen_addresses = '*'            # what IP address(es) to listen on;
port = 11924                            # (change requires restart)
gtm_host = 'adpg1'                  # Host name or address of GTM
gtm_port = 14924                        # Port of GTM
pgxc_node_name = 'coord11924'                   # Coordinator or Datanode name
sequence_range = 100
pooler_port = 6670
default_transaction_isolation = 'read committed'
default_transaction_read_only = off
</code></pre>

9.两台机器配置连接权限
找到对应节点文件夹下面的pg_hba文件,添加访问权限
<pre><code>
host all all 127.0.0.1/32 trust
host all all 10.100.5.0/24 trust
</code></pre>

10.启动服务，启动顺序 gtm->gtmprox->datanode->coordinator
 <pre><code>
 80机器：
gtm_ctl start -Z gtm -D /home/dongjie/gtm13921 > /data1/gtm21 2>&1
gtm_ctl start -Z gtm_proxy -D /home/dongjie/gtmprox14921 > /data1/gtmprox21 2>&1
gtm_ctl start -Z gtm_proxy -D /home/dongjie/gtmprox14922 > /data1/gtmprox22 2>&1
pg_ctl start -Z datanode -D /home/dongjie/datanode12921 > /data1/data1 2>&1
pg_ctl start -Z datanode -D /home/dongjie/datanode12922 > /data1/data2 2>&1
pg_ctl start -Z coordinator -D /home/dongjie/coord11921 > /data1/coord1 2>&1
pg_ctl start -Z coordinator -D /home/dongjie/coord11922 > /data1/coord2 2>&1
 81机器：
gtm_ctl start -Z gtm_proxy -D /home/dongjie/gtmprox14923 > /data1/gtmprox23 2>&1
gtm_ctl start -Z gtm_proxy -D /home/dongjie/gtmprox14924 > /data1/gtmprox24 2>&1
pg_ctl start -Z datanode -D /home/dongjie/datanode12923 > /data1/data23 2>&1
pg_ctl start -Z datanode -D /home/dongjie/datanode12924 > /data1/data22 2>&1
pg_ctl start -Z coordinator -D /home/dongjie/coord11923 > /data1/coord123 2>&1
pg_ctl start -Z coordinator -D /home/dongjie/coord11924 > /data1/coord24 2>&1
 </code></pre>
 
11.在各个datanode和coordinator节点添加集群节点配置
<pre><code>
分别在 11921 11922 11923 11924 12921 12922 12923 12924八个节点上执行下面操作
psql -h 127.0.0.1 -p 11921 -U postgres postgres

delete from pgxc_node;
create node coord11921 with (type=coordinator, host='10.100.5.80', port=11921);
create node coord11922 with (type=coordinator, host='10.100.5.80', port=11922);
create node coord11921 with (type=coordinator, host='10.100.5.81', port=11923);
create node coord11922 with (type=coordinator, host='10.100.5.81', port=11924);
create node datanode12921 with (type=datanode, host='10.100.5.80', port=12921, primary=false);
create node datanode12922 with (type=datanode, host='10.100.5.80', port=12922, primary=false);
create node datanode12923 with (type=datanode, host='10.100.5.81', port=12923, primary=false);
create node datanode12924 with (type=datanode, host='10.100.5.81', port=12924, primary=false);
</code></pre>