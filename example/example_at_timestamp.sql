--  Create schema and tables, enable history
drop schema if exists example cascade;
create schema example;

create table example.customer(
  id int primary key,
  name varchar(100) not null
);
insert into example.customer values (1,'Horns'),(2,'Hooves');

create table example.document(
  id int primary key,
  number varchar(10),
  date date
);

create table example.invoice(
  primary key (id),
  customer_id int references example.customer(id),
  amount numeric(20,2)
) inherits (example.document);
create index on example.invoice(customer_id);

call pghist.hist_enable('example', 'customer');
call pghist.hist_enable('example', 'document');
call pghist.hist_enable('example', 'invoice');

-- Enter data into tables   
insert into example.document values (11,'#10', current_date);
insert into example.invoice  values (12,'#20', current_date,   1, 120.00);
insert into example.invoice  values (13,'#30', current_date-1, 2, 130.00);

-- Select data in past
select * from example.document_at_timestamp(now()-interval '10 second');

-- Erroneous update and recovery
update example.invoice set customer_id=2,amount=300 where date=current_date;

update example.invoice i
    set amount = h.amount
  from example.invoice_at_timestamp('2024-04-06 10:00:00') h
  where i.id = h.id
    and i.date=current_date;

-- Сombination log and versioning   
select * 
  from example.invoice_at_timestamp('2024-04-06 10:00:00')
  where id in (
          select id from example.invoice_hist
            where hist_timestamp>'2024-04-06 10:00:00'
              and hist_db_user=current_user
        );

-- Сomplex query in past        
select * 
  from example.invoice i
  join example.customer c on c.id=i.customer_id;
 
do $$
begin
  perform set_config('pghist.at_timestamp', '2024-04-06 10:00:00', true);	
  perform example.invoice_at_timestamp();	
  perform example.customer_at_timestamp();
end; 
$$;
 
select * 
  from example_invoice_at_timestamp i
  join example_customer_at_timestamp c on c.id=i.customer_id;
