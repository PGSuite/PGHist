-- 1. Developer create schema and tables, enable history, change default desc, grant privileges to user

drop schema if exists example cascade;
create schema example;

create table example.document(
  id int primary key,
  number varchar(10),
  date date
);
comment on table example.document is 'Document';

create table example.invoice(
  primary key (id),
  amount numeric(20,2) 
) inherits (example.document);
comment on table example.invoice is 'Invoice';
comment on column example.invoice.id is 'Identifier';
comment on column example.invoice.number is 'Number';
comment on column example.invoice.date is 'Date';
comment on column example.invoice.amount is 'Amount';

create table example.product(
  id int primary key,
  name varchar(100) not null,
  code varchar(10) not null unique
);
comment on table example.product is 'Product';
comment on column example.product.name is 'Name';
comment on column example.product.code is 'Code';
 
create table example.invoice_product(
  id serial primary key,
  invoice_id int references example.invoice(id),   
  product_id int references example.product(id),
  quantity int
);
comment on table example.invoice_product is 'Product of invoice';
comment on column example.invoice_product.id is 'Identifier';
comment on column example.invoice_product.invoice_id is 'Invoice';
comment on column example.invoice_product.product_id is 'Product';
comment on column example.invoice_product.quantity is 'Quantity';

create index on example.invoice_product(invoice_id); 

call pghist.hist_enable('example', 'document');
call pghist.hist_enable('example', 'invoice');
call pghist.hist_enable('example', 'product');
call pghist.hist_enable('example', 'invoice_product');

-- set description expression for row and column, required for understandable display to the user
call pghist.hist_row_desc_expression('example', 'invoice', null);   
call pghist.hist_row_desc_expression('example', 'invoice_product', '''product row #''||row.id');
call pghist.hist_column_desc_expression('example', 'invoice_product', 'product_id', 'select name||'' [''||code||'']'' from example.product where id=row.product_id');
 
grant usage on schema example to user_1;  
grant select,insert,update,delete on example.document,example.invoice,example.product,example.invoice_product to user_1;
grant select on example.document_hist,example.invoice_hist,example.product_hist,example.invoice_product_hist to user_1;  
grant execute on function 
  example.document_changes,example.document_at_timestamp,
  example.invoice_changes,example.invoice_at_timestamp,
  example.product_changes,example.product_at_timestamp,
  example.invoice_product_changes,example.invoice_product_at_timestamp  
  to user_1;

-- 2. User fills in tables, looks at tables in the past and their changes
insert into example.document values (11,'#10', current_date);
insert into example.invoice  values (12,'#20', current_date, 120.00);
insert into example.product(id,name,code) values (101,'Pensil','030'),(102,'Notebook','040');
insert into example.invoice_product(id, invoice_id, product_id, quantity) values (1,12,101,1000),(2,12,101,10);

do $$
begin
  update example.invoice_product set product_id=102,quantity=quantity+1 where id=1;
  update example.invoice set amount=220 where id=12;
end; 
$$;

-- full info
select * from example.document_hist;
select * from example.document_changes(); 
select * from example.document_at_timestamp(now()-interval '15 second');

-- selected info
select * from example.document_hist where id=12;
select * from example.document_changes('id=$1',12);
select * from example.document_at_timestamp(now()-interval '15 second') where id=12;

-- changes in two related tables, fast execution provides index invoice_product(invoice_id)
-- variant 1, simple 
select * from example.invoice_changes('id=$1',12)
union all
select * from example.invoice_product_changes('invoice_id=12')
-- variant 2, for display in interface, recommended to wrap the query in a stored procedure  
select timestamp,db_user,operation,row_desc,column_comment,coalesce(value_old_desc,value_old) old_value,coalesce(value_new_desc,value_new) new_value
  from (
    select * from example.invoice_changes('id=$1',12) where column_name not in ('id')
    union all
    select * from example.invoice_product_changes('invoice_id=$1',12) where column_name not in ('id', 'invoice_id')
  ) c
  order by hist_id desc,column_pos
