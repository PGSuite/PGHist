-- 1. Developer create schema and tables, enable history, grant privileges to user

drop schema if exists test cascade;
create schema test;

create table test.document(
  id int primary key,
  number varchar(10),
  date_ date
);
comment on table test.document is 'Document';

create table test.invoice(
  primary key (id),
  amount numeric(20,2) 
) inherits (test.document);
comment on table test.invoice is 'Invoice';
comment on column test.invoice.id is 'Identifier';
comment on column test.invoice.number is 'Number';
comment on column test.invoice.date_ is 'Date';
comment on column test.invoice.amount is 'Amount';
 
create table test.invoice_product(
  id serial primary key,
  invoice_id int references test.invoice(id),   
  product text,
  quantity int
);
comment on table test.invoice_product is 'Product of invoice';
comment on column test.invoice_product.id is 'Identifier';
comment on column test.invoice_product.invoice_id is 'Invoice';
comment on column test.invoice_product.product is 'Product';
comment on column test.invoice_product.quantity is 'Quantity';


call pghist.hist_enable('test', 'document');
call pghist.hist_enable('test', 'invoice');
call pghist.hist_enable('test', 'invoice_product');

grant usage on schema test to user_1;  
grant select,insert,update,delete on test.document,test.invoice,test.invoice_product to user_1;
grant execute on function 
  test.document_changes,test.document_at_timestamp,
  test.invoice_changes,test.invoice_at_timestamp,
  test.invoice_product_changes,test.invoice_product_at_timestamp  
  to user_1;

-- 2. User fills in tables, looks at tables in the past and their changes
insert into test.document values (1,'#10', current_date);
insert into test.invoice values (2,'#20',current_date,120.00);
insert into test.invoice_product values (1,2,'Pensil',1000);
insert into test.invoice_product values (2,2,'Notebook',100);

do $$
begin
  update test.invoice_product set quantity=2000 where id=1;
  update test.invoice set amount=220 where id=2;
end; 
$$;

select * from test.document_at_timestamp(now()-interval '15 second');
select * from test.document_changes();

-- Changes in two related tables to display in interface
select transaction_timestamp,db_user,operation,table_comment,row_comment,column_comment,value_old,value_new from (
  select * from test.invoice_changes('id=$1',2)
  union all
  select * from test.invoice_product_changes(where_clause=>'invoice_id=2', row_comment_clause=>'product')  where column_name not in ('id', 'invoice_id')
) c
order by hist_id,column_pos
