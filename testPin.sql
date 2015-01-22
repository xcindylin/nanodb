select * from t;
insert into t values(5);
update t set a = 6 where a = 5;
select * from t;
select * from t where a < 10;
delete from t where a < 10;
exit;
