drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t2(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;
create table t3(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '../../data/files/cbo_t1.txt' into table t1;
load data local inpath '../../data/files/cbo_t2.txt' into table t2;
load data local inpath '../../data/files/cbo_t3.txt' into table t3;

set hive.stats.dbclass=jdbc:derby;
analyze table t1 compute statistics;
analyze table t1 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table t2 compute statistics;
analyze table t2 compute statistics for columns key, value, c_int, c_float, c_boolean;
analyze table t3 compute statistics;
analyze table t3 compute statistics for columns key, value, c_int, c_float, c_boolean;

set hive.stats.fetch.column.stats=true;


-- Test Basic Inner Joins
select t1.key, t2.value from t1 join t2 on t1.key=t2.key join t3 on t1.key=t3.key order by t1.key, t2.value;

select t1.key, t2.value, t3.key from t1 join t2 on t1.key=t2.key and t1.c_int=t2.c_int and t1.c_float = t2.c_float join t3 on t1.key=t3.key where ((t1.key > 0 or t2.key > 0) or ((t1.c_float + t3.c_float) > 1)) and (t1.key > 0 or t2.key > 0) and (t1.c_float + t2.c_float > 1) order by t1.key, t2.value;



-- Test Inner Joins with Subqueries
select t1.key, t2.value from (select key, value, c_float, c_int from t1 )t1 join (select key, value, c_float, c_int from t2 )t2 on t1.key=t2.key and t1.c_int=t2.c_int and t1.c_float = t2.c_float join (select key, value, c_float, c_int from t3 )t3 on t1.key=t3.key and t1.c_int=t3.c_int and t1.c_float=t3.c_float where ((t1.key > 0 or t2.key > 0) and (t1.c_float + t2.c_float > 1) and t3.c_float > 0.0) or (t1.key > 0 or t2.key > 0) or (t1.c_float + t3.c_float > 1) order by t1.key, t2.value;



-- Test  Inner Joins with Subqueries using predicates, GB
select t1.key, t2.value, t3.key from (select key, value, c_float, c_int from t1 where t1.c_float*10 > c_int group by key, value, c_float, c_int having  c_int >= 1 )t1 join (select key, value, c_float, c_int from t2 where t2.c_float*10 > c_int group by key, value, c_float, c_int having c_int >= 1 )t2 on t1.key=t2.key  and t1.c_int=t2.c_int and t1.c_float = t2.c_float join (select key, value, c_float, c_int from t3 where t3.c_float*10 > c_int group by key, value, c_float, c_int having (c_int >= 1) )t3 on t1.key=t3.key  and t1.c_int=t3.c_int and t1.c_float=t3.c_float where (t1.key > 0 or t2.key > 0) or (t1.c_float + t2.c_float > 1) order by t1.key, t2.value, t3.key;



-- Test  Inner Joins with Subqueries + outer queries using predicates, GB, OB, Limit
select t1.key, t2.value from (select key, value, c_float from t1 where t1.c_float*10 > c_int )t1 join (select key, value, c_float, c_int from t2 where t2.c_float*10 > c_int)t2 on t1.key=t2.key  join (select key, value, c_float from t3 where t3.c_float*10 > c_int  )t3 on t1.key=t3.key  where (t1.key > 0 or t2.key > 0) or (t1.c_float + t2.c_float > 1) group by t1.key, t2.value, t2.c_float, t2.c_int  having t2.c_int >= 1 order by t1.key, t2.value limit 10;



-- Test  Inner Joins with Subqueries using predicates, GB + outer queries using predicates, GB, OB, LIMIT
select t1.key, t2.value from (select key, value, c_float, c_int from t1 where t1.c_float*10 > c_int group by key, value, c_float, c_int  having c_int >= 1 )t1 join (select key, value, c_float, c_int from t2 where t2.c_float*10 > c_int  group by key, value, c_float, c_int )t2 on t1.key=t2.key and t1.c_int=t2.c_int and t1.c_float=t2.c_float  join (select key, value, c_float from t3 where t3.c_float*10 > c_int  group by key, value, c_float )t3 on t1.key=t3.key group by t1.key, t2.value, t2.c_float, t3.key, t2.c_int  having t2.c_int >= 1 and t3.key >= 1 order by t1.key,t2.value limit 10;

