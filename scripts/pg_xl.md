pg_xl 部署
=======

部署机器：10.100.5.80

部署环境：centos-x64

部署软件版本： postgres-xl-v9.2

部署规划：一个gtm主节点 两个gtm-proxy节点 两个coordinator节点 两个datanode节点

1.添加hosts
<pre><code>
10.100.5.80	adpg0
</code></pre>

2.安装pg_xl:
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
cd /home/dongjie
mkdir datanode12921 datanode12922 cood11921 cood11922 gtm13921 gtmprox14921 gtmprox14922
mkdir datanode12923 datanode12924 cood11923 cood11924 gtm13922 gtmprox149213 gtmprox14924
</code></pre>

4.配置.bash_profile文件
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

5.配置gtm节点
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

vim /home/dongjie/gtmprox14921/gtm_proxy.conf

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
</code></pre>

7.配置datanode节点
<pre><code>
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
</code></pre>

8.配置coordinator节点
<pre><code>
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
</code></pre>

9.配置连接权限
找到对应节点文件夹下面的pg_hba文件,添加访问权限
<pre><code>
host all all 127.0.0.1/32 trust
host all all 10.100.5.0/24 trust
</code></pre>

10.启动服务，启动顺序 gtm->gtmprox->datanode->coordinator
 <pre><code>
gtm_ctl start -Z gtm -D /home/dongjie/gtm13921 > /data1/gtm21 2>&1
gtm_ctl start -Z gtm_proxy -D /home/dongjie/gtmprox14921 > /data1/gtmprox21 2>&1
gtm_ctl start -Z gtm_proxy -D /home/dongjie/gtmprox14922 > /data1/gtmprox22 2>&1
pg_ctl start -Z datanode -D /home/dongjie/datanode12921 > /data1/data1 2>&1
pg_ctl start -Z datanode -D /home/dongjie/datanode12922 > /data1/data2 2>&1
pg_ctl start -Z coordinator -D /home/dongjie/coord11921 > /data1/coord1 2>&1
pg_ctl start -Z coordinator -D /home/dongjie/coord11922 > /data1/coord2 2>&1
 </code></pre>
 
11.在各个datanode和coordinator节点添加集群节点配置
<pre><code>
psql -h 127.0.0.1 -p 11921 -U postgres postgres

delete from pgxc_node;
create node coord11921 with (type=coordinator, host='10.100.5.80', port=11921);
create node coord11922 with (type=coordinator, host='10.100.5.80', port=11922);
create node datanode12921 with (type=datanode, host='10.100.5.80', port=12921, primary=false);
create node datanode12922 with (type=datanode, host='10.100.5.80', port=12922, primary=false);
</code></pre>