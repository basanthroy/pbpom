1)

$ PYTHONPATH="${PYTHONPATH}:/opt/dwradiumone/arte/app/data_prepare/r1-dw-arte-app/" ; export PYTHONPATH

2)

$ /opt/python2.7/bin/python ./r1-dw-arte-app/application/src/main/python/com/radiumone/arte/fwk/data/prepare/job/enqueue.py 1 2 901
$ /opt/python2.7/bin/python ./r1-dw-arte-app/application/src/main/python/com/radiumone/arte/fwk/data/prepare/job/enqueue2.py
$ /opt/python2.7/bin/python ./r1-dw-arte-app/application/src/main/python/com/radiumone/arte/fwk/data/prepare/job/dequeue.py
note: add -prod to enqueue.py and dequeue.py for the production run

/*************************************************************************************************************************/
/*** appendix ************************************************************************************************************/
/*************************************************************************************************************************/

1)

use MySQLdb, instead of mysql.connector

[root@jobserver2 ~]# rpm -qa| grep r1 | grep -i mysql
r1-python-MySQLdb-1.2.3-5.el6.x86_64
r1-python-PyMySQL-0.5-5.el6.x86_64

http://mysql-python.sourceforge.net/MySQLdb.html

2)

[4/6/16, 4:33:49 PM] Chu Chi Liu: hi
[4/6/16, 4:34:03 PM] Chu Chi Liu: i noticed someone trying to do the following … 
[arteu@jobserver2 r1-dw-arte-app]$ cd ../test/r1-dw-arte-client/
[arteu@jobserver2 r1-dw-arte-client]$ git remote -v
origin	ssh://git@git.dev.dw.sc.gwallet.com:7999/dw/r1-dw-arte-client.git (fetch)
origin	ssh://git@git.dev.dw.sc.gwallet.com:7999/dw/r1-dw-arte-client.git (push)
[arteu@jobserver2 r1-dw-arte-client]$
[4/6/16, 4:34:18 PM] Chu Chi Liu: do you know this git user ?
[4/6/16, 4:52:43 PM] Dmitriy Khan: service user
[4/6/16, 4:52:58 PM] Dmitriy Khan: don't worry about it
[4/6/16, 4:53:02 PM] Dmitriy Khan: use git and your repos

3)

[arteu@jobserver2 ~]$ git clone
ssh://git@git.dev.dw.sc.gwallet.com:7999/dw/r1-dw-arte-app.git
Initialized empty Git repository in /home/arteu/r1-dw-arte-app/.git/
remote: Counting objects: 1266, done.
remote: Compressing objects: 100% (1242/1242), done.
remote: Total 1266 (delta 1048), reused 0 (delta 0)
Receiving objects: 100% (1266/1266), 5.58 MiB | 10.55 MiB/s, done.
Resolving deltas: 100% (1048/1048), done.
[arteu@jobserver2 ~]$ git clone
ssh://git@git.dev.dw.sc.gwallet.com:7999/dw/r1-dw-arte-algo.git
Initialized empty Git repository in /home/arteu/r1-dw-arte-algo/.git/
remote: Counting objects: 247, done.
remote: Compressing objects: 100% (135/135), done.
remote: Total 247 (delta 87), reused 210 (delta 73)
Receiving objects: 100% (247/247), 71.68 KiB, done.
Resolving deltas: 100% (87/87), done.
[arteu@jobserver2 ~]$ ls -lrt
total 28
drwxrwxr-x 5 arteu arteu 4096 Apr  4 19:49 credentials
drwxrwxr-x 3 arteu arteu 4096 Apr  6 15:18 test
drwxrwxr-x 4 arteu arteu 4096 Apr  6 16:37 delete_me_later
drwxrwxr-x 3 arteu arteu 4096 Apr  6 19:07 tmp
-rw-r--r-- 1 arteu arteu 1812 Apr  7 10:56 readme.txt
drwxr-xr-x 3 arteu arteu 4096 Apr  7 10:59 r1-dw-arte-app
drwxr-xr-x 3 arteu arteu 4096 Apr  7 10:59 r1-dw-arte-algo
[arteu@jobserver2 ~]$

4)

CREATE EXTERNAL TABLE output_log
(
strategy_id int,
dimension_structs array<struct<dimension_type_id:int,dimension_value:string>>,
probability_score double,
min_bid double,
max_bid double,
timestamp bigint,
multiplier double
)
PARTITIONED BY (algo_id int, dt int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/arteu/model/test/log/';

CREATE EXTERNAL TABLE output_log(strategy_id int,dimension_structs array<struct<dimension_type_id:int,dimension_value:string>>,probability_score double,min_bid double,max_bid double,timestamp bigint,multiplier double)PARTITIONED BY (algo_id int, dt int) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/user/arteu/model/test/log/';

5)

hive> use chuchi_tmp ;
OK
Time taken: 0.026 seconds
hive> msck repair table output_log;
OK
Partitions not in metastore:
output_log:algo_id=1/dt=20160408
output_log:algo_id=2/dt=20160408
Repair: Added partition to metastore output_log:algo_id=1/dt=20160408
Repair: Added partition to metastore output_log:algo_id=2/dt=20160408
Time taken: 0.183 seconds, Fetched: 3 row(s)
hive> select * from chuchi_tmp.output_log limit 5 ;
OK
552211	[{"dimension_type_id":1,"dimension_value":"10"}]	0.616059701	0.01	1.0	NULL	0.0174493671	1	20160408
552211	[{"dimension_type_id":1,"dimension_value":"12"}]	1.1542282682	0.01	1.0	NULL	0.0239623684	1	20160408
552211	[{"dimension_type_id":1,"dimension_value":"16"}]	0.0339978698	0.01	1.0	NULL	0.0104051626	1	20160408
552211	[{"dimension_type_id":1,"dimension_value":"20"}]	1.1312740006	0.01	1.0	NULL	0.0236845722	1	20160408
552211	[{"dimension_type_id":1,"dimension_value":"23"}]	0.0817230319	0.01	1.0	NULL	0.0109827401	1	20160408
Time taken: 0.174 seconds, Fetched: 5 row(s)
hive>

6)

hive> dfs -ls /user/arteu/model/test/log ;
Found 2 items
drwxr-xr-x   - arteu hadoop          0 2016-04-08 11:43	/user/arteu/model/test/log/algo_id=1
drwxr-xr-x   - arteu hadoop          0 2016-04-08 11:46	/user/arteu/model/test/log/algo_id=2
hive>

7)

> select distinct strategy_id, algo_id from arte_dimension_predictions;
Total MapReduce jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapred.reduce.tasks=<number>
Starting Job = job_201604021855_135425, Tracking URL = http://namenode3.dw.sc.gwallet.com:50030/jobdetails.jsp?jobid=job_201604021855_135425
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_201604021855_135425
Hadoop job information for Stage-1: number of mappers: 3; number of reducers: 1
2016-04-08 17:48:33,619 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.49 sec
MapReduce Total cumulative CPU time: 9 seconds 490 msec
Ended Job = job_201604021855_135425
MapReduce Jobs Launched:
Job 0: Map: 3  Reduce: 1   Cumulative CPU: 10.72 sec   HDFS Read: 1090863 HDFS Write: 29 SUCCESS
Total MapReduce CPU Time Spent: 10 seconds 720 msec
OK
542378	901
552211	1
584795	2
Time taken: 704.797 seconds, Fetched: 3 row(s)

8)

curl -H "Content-Type: application/json" http://dt02.etl.dw.sc.gwallet.com:8080/DWServices-test/arte/app/enqueue -d '{"algoId":1001}'