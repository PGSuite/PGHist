-- Create schema and table, enable history, change data 
drop schema if exists example cascade;
create schema example;

create table example.document(
  id int primary key,
  number varchar(10),
  amount numeric(10,2)
);

call pghist.hist_enable('example', 'document');

insert into example.document values (11, '#10', 100);
insert into example.document values (12, '#20', 200);
update example.document set number='#20/2',amount=210 where id=12;
update example.document set amount=220 where id=12;
delete from example.document where id=11;

-- Select full info
select * from example.document_hist;

-- Select partial info by id
select hist_timestamp,hist_operation,hist_db_user,amount_old 
  from example.document_hist
  where id=12 and (hist_operation!='UPDATE' or 'amount'=any(hist_update_columns))
  order by hist_statement_id;

-- Select partial info with current values
select * from example.document_hist
union all 
select null,null,null,'[CURRENT_VALUES]',null,null,null,null,null,null,* from example.document
order by id,1

-- Create extended view and grant select on it to developer
-- (alternatively to 'grant select on all tables in schema pghist to developer');  
select *
  from pghist.hist_data$example_document h  
  join pghist.hist_statement s on s.id = h.hist_statement_id
  join pghist.hist_query q on q.hash = s.query_hash
  join pghist.hist_transaction t on t.id = s.transaction_id
  order by h.hist_statement_id, h.id;
 
create or replace view example.document_hist_ext as  
  select h.id,t.timestamp_commit,t.db_client_addr,q.text query_text
    from pghist.hist_data$example_document h  
    join pghist.hist_statement s on s.id = h.hist_statement_id
    join pghist.hist_query q on q.hash = s.query_hash
    join pghist.hist_transaction t on t.id = s.transaction_id
    order by h.hist_statement_id, h.id;
   
select * from example.document_hist_ext where id=12;   

grant select on example.document_hist_ext to developer_1;      

