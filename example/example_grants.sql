-- postgres grant privileges on pghist schema and pghist.hist_enable procedure to developer_1
-- all privileges in pghist_grants.sql file
grant usage on schema pghist to developer_1;
grant execute on procedure pghist.hist_enable(name) to developer_1;


-- developer_1 create table, enable history, grant privileges on example table and example_changes function to user_1
create table example(
  id int primary key,
  name varchar(20),
  number numeric(10,2),
  date date
);

call pghist.hist_enable('example');
  
grant select,insert,update on example to user_1;
grant execute on function example_changes to user_1;


-- user_1 change data and view changes 
insert into example values (1, 'Example', 10, current_date);
update example set number=20, date=date-1;

select * from example_changes() order by 1,2,3;

-- When trying to select log, user_1 receives an error
-- SQL Error [42501]: ERROR: permission denied for view example_hist 
select * from example_hist;