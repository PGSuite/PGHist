-- Developer create schema and tables, enable history, change default desc, grant privileges to user
drop schema if exists example cascade;
create schema example;

create table example.customer(
  id int primary key,
  name varchar(100) not null
);
insert into example.customer values (1,'Horns'),(2,'Hooves');

create table example.invoice(
  id int primary key,  
  number varchar(10),
  date date,  
  customer_id int references example.customer(id),
  amount numeric(20,2) 
);
comment on table example.invoice is 'Invoice';
comment on column example.invoice.id is 'Identifier';
comment on column example.invoice.number is 'Number';
comment on column example.invoice.date is 'Date';
comment on column example.invoice.customer_id is 'Сustomer';
comment on column example.invoice.amount is 'Amount';

create table example.product(
  id int primary key,
  name varchar(100) not null,
  code varchar(10) not null
);

create table example.invoice_product(
  id serial primary key,
  invoice_id int references example.invoice(id),   
  product_id int references example.product(id),  
  quantity int,
  color char(1) check (color in ('R','G','B'))  
);
comment on table example.invoice_product is 'Product of invoice';
comment on column example.invoice_product.id is 'Identifier';
comment on column example.invoice_product.invoice_id is 'Invoice';
comment on column example.invoice_product.product_id is 'Product';
comment on column example.invoice_product.quantity is 'Quantity';
comment on column example.invoice_product.color is 'Color';
create index on example.invoice_product(invoice_id);

grant usage on schema example to user_1;  
grant select,insert,update,delete on example.invoice,example.product,example.invoice_product to user_1;

call pghist.hist_enable('example', 'invoice');
call pghist.hist_enable('example', 'invoice_product', 'example', 'invoice');

grant execute on function example.invoice_changes,example.invoice_product_changes to user_1;

-- User fills in tables, looks changes
insert into example.invoice values (12,'#20', current_date, 1, 120.00);
update example.invoice set customer_id=2 where id=12;
insert into example.product(id,name,code) values (101,'Pensil','030'),(102,'Notebook','040');
insert into example.invoice_product(id, invoice_id, product_id, quantity, color) values (1,12,101,1000,'R'),(2,12,101,10,'G');

do $$
begin
  update example.invoice_product set quantity=quantity+1,color='B' where id=1;
  delete from example.invoice_product where id=2;
  update example.invoice set amount=150 where id=12;
end; 
$$;

-- Select full data, first three columns provide chronological
select * from example.invoice_changes() order by 1,2,3;

-- Select full data with column id (immutable), insert detail by columns  
select * from example.invoice_changes(insert_detail=>true, columns_immutable=>true) order by 1,2,3; 

-- Select partial data by id
select * from example.invoice_changes('id=12') order by 1,2,3;

-- Select changes in two related tables, fast execution provides index invoice_product(invoice_id)
select * from example.invoice_changes('id=$1',12)
union all
select * from example.invoice_product_changes('invoice_id=$1', 12)
order by 1,2,3

-- Developer create function that displays information in custom format for user
create or replace function example.invoice_changes_ui(id int) returns setof pghist.table_change language sql security definer as $$
  select * from example.invoice_changes('id=$1',id,insert_detail=>true)
  union all
  select * from example.invoice_product_changes('invoice_id=$1', id)
  order by 1,2,3;
$$;

-- Set description expression for columns row_desc and value_desc   
call pghist.hist_expression_row_desc('example', 'invoice', '''Invoice''');   
call pghist.hist_expression_row_desc('example', 'invoice_product', $$ 'Row #'||$1.id||' / '||(select name from example.product where id=$1.product_id) $$);
call pghist.hist_expression_value_desc('example', 'invoice_product', 'color', $$ case when $1='R' then 'Red' when $1='B' then 'Blue' when $1='G' then 'Green' else $1 end $$);

-- Replace function for column db_user_name
create or replace function example.db_user_name(db_user name) returns varchar language plpgsql as $$ 
begin 
  return '['||db_user||']';                                        
end; $$;

call pghist.hist_column_custom_function('db_user_name', 'example.db_user_name');

-- Select changes for user interface 
select * from example.invoice_changes_ui(12);
select timestamp,operation_name,db_user_name,row_desc,column_comment,value_old_desc,value_new_desc from example.invoice_changes_ui(12);

