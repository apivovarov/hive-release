CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '/Users/vikram/src/hive/hive-trunk/data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '/Users/vikram/src/hive/hive-trunk/data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '/Users/vikram/src/hive/hive-trunk/data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '/Users/vikram/src/hive/hive-trunk/data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '/Users/vikram/src/hive/hive-trunk/data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '/Users/vikram/src/hive/hive-trunk/data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');

set hive.optimize.bucketmapjoin = true;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.input.format = org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.optimize.bucketmapjoin = true;
set hive.auto.convert.join.use.nonstaged = false;

explain extended
select a.key, a.value, b.value
from srcbucket_mapjoin a join srcbucket_mapjoin_part b
on a.key=b.key;

select a.key, a.value, b.value
from srcbucket_mapjoin a join srcbucket_mapjoin_part b
on a.key=b.key;
