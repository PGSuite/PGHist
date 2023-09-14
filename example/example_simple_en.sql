-- Create table
create table example(
  id int primary key,
  name varchar(20),
  number numeric(10,2),
  date date
);

-- Endable keeping history
call pghist.hist_enable('example');

-- Change table
insert into example values (1, 'Example', 10, current_date);
update example set number=20, date=date-1;

-- View table at timestamp 
select * from example_at_timestamp(now()-interval '10 second');

-- View changes by fields
select * from example_changes();

-- drop table example cascade;