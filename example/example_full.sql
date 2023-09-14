-- 1. Developer create schema and tables, enable history, change default desc, grant privileges to user

drop schema if exists test cascade;
create schema test;

create table test.document(
  id int primary key,
  number varchar(10),
  date date
);
comment on table test.document is 'Document';

create table test.invoice(
  primary key (id),
  amount numeric(20,2) 
) inherits (test.document);
comment on table test.invoice is 'Invoice';
comment on column test.invoice.id is 'Identifier';
comment on column test.invoice.number is 'Number';
comment on column test.invoice.date is 'Date';
comment on column test.invoice.amount is 'Amount';

create table test.product(
  id int primary key,
  name varchar(100) not null,
  code varchar(10) not null unique
);
comment on table test.product is 'Product';
comment on column test.product.name is 'Name';
comment on column test.product.code is 'Code';
 
create table test.invoice_product(
  id serial primary key,
  invoice_id int references test.invoice(id),   
  product_id int references test.product(id),
  quantity int
);
comment on table test.invoice_product is 'Product of invoice';
comment on column test.invoice_product.id is 'Identifier';
comment on column test.invoice_product.invoice_id is 'Invoice';
comment on column test.invoice_product.product_id is 'Product';
comment on column test.invoice_product.quantity is 'Quantity';

create index on test.invoice_product(invoice_id); 

call pghist.hist_enable('test', 'document');
call pghist.hist_enable('test', 'invoice');
call pghist.hist_enable('test', 'product');
call pghist.hist_enable('test', 'invoice_product');

-- select pghist.hist_row_desc_expression_current('test', 'invoice_product');
call pghist.hist_row_desc_expression('test', 'invoice_product', '''Invoice row #''||row.id');

call pghist.hist_column_desc_expression('test', 'invoice_product', 'product_id', 'select name||'' [''||code||'']'' from test.product where id=row.product_id');
-- select pghist.hist_column_desc_expression_current('test', 'invoice_product', 'product_id'), pghist.hist_column_desc_expression_default('test', 'invoice_product', 'product_id');
 

grant usage on schema test to user_1;  
grant select,insert,update,delete on test.document,test.invoice,test.product,test.invoice_product to user_1;
grant execute on function 
  test.document_changes,test.document_at_timestamp,
  test.invoice_changes,test.invoice_at_timestamp,
  test.product_changes,test.product_at_timestamp,
  test.invoice_product_changes,test.invoice_product_at_timestamp  
  to user_1;

-- 2. User fills in tables, looks at tables in the past and their changes
insert into test.document values (11,'#10', current_date);
insert into test.invoice  values (12,'#20', current_date, 120.00);
insert into test.product(id,name,code) values (101,'Pensil','030'),(102,'Notebook','040');
insert into test.invoice_product(id, invoice_id, product_id, quantity) values (1,12,101,1000),(2,12,101,10);

do $$
begin
  update test.invoice_product set product_id=102,quantity=quantity+1 where id=1;
  update test.invoice set amount=220 where id=12;
end; 
$$;

-- full info
select * from test.document_changes(); 
select * from test.document_at_timestamp(now()-interval '15 second');

-- selected info
select * from test.document_changes('id=$1',12);
select * from test.document_at_timestamp(now()-interval '15 second') where id=12;

-- Changes in two related tables to display in interface, fast execution provides index invoice_product(invoice_id)
select transaction_timestamp,row_desc,column_comment,coalesce(value_old_desc,value_old) old_value,coalesce(value_new_desc,value_new) new_value,operation,db_user
  from (
    select * from test.invoice_changes('id=$1',12)
    union all
    select * from test.invoice_product_changes('invoice_id=12') where column_name not in ('id', 'invoice_id')
  ) c
  order by hist_id desc,column_pos
