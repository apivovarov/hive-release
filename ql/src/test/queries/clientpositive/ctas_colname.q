-- HIVE-4392, column alias from expressionRR (GBY, etc.) is not valid column name for table
-- use internal name as col name

-- group by
explain
create table summary as select *, sum(key), count(value) from src;
create table summary as select *, sum(key), count(value) from src;
describe formatted summary;
select * from summary;

-- window functions
explain
create table x4 as select *, rank() over(partition by key order by value) as rr from src1;
create table x4 as select *, rank() over(partition by key order by value) as rr from src1;
describe formatted x4;
select * from x4;

explain
create table x5 as select *, lead(key,1) over(partition by key order by value) from src limit 20;
create table x5 as select *, lead(key,1) over(partition by key order by value) from src limit 20;
describe formatted x5;
select * from x5;

-- sub query
explain
create table x6 as select * from (select *, max(key) from src1) a;
create table x6 as select * from (select *, max(key) from src1) a;
describe formatted x6;
select * from x6;
