-- Create function that returns changes by date
create or replace function example.invoice_changes_ui_date(date_changes date) returns setof pghist.table_change language sql security definer as $$
  select c.*
    from (select distinct id from example.invoice_hist where hist_timestamp::date=date_changes) h
    cross join example.invoice_changes(h.id) c
  order by 1,2,3;
$$;
select * from example.invoice_changes_ui_date(current_date);

-- Create function that returns changes to master and detail tables
create or replace function example.invoice_changes_ui_simple(id int) returns setof pghist.table_change language sql security definer as $$
  select * from example.invoice_changes(id=>id,hist_tables_detail=>false,hist_columns_insert=>true)
  union all
  select * from example.invoice_product_changes(invoice_id=>id)
  order by 1,2,3;
$$;
select * from example.invoice_changes_ui_simple(12);

-- Create type with only necessary columns for the UI and cast function to it,
-- type is created only once (as a rule, there is only one history view form per project) 
create type example.table_change_ui as (
  timestamp timestamptz,
  operation_name varchar,
  db_user_name varchar,
  row_desc text,  
  column_comment varchar,      
  value_old_desc text,      
  value_new_desc text
);
create or replace function example.table_change_ui_cast(tc pghist.table_change) returns example.table_change_ui language plpgsql as $$
begin
  return (tc.timestamp,tc.operation_name,tc.db_user_name,tc.row_desc,tc.column_comment,tc.value_old_desc,tc.value_new_desc)::example.table_change_ui; 
end; $$;
create cast(pghist.table_change as example.table_change_ui) with function example.table_change_ui_cast as assignment;

-- Create function that returns only necessary columns
create or replace function example.invoice_changes_ui(id int) returns setof example.table_change_ui language sql security definer as $$
  select tc::pghist.table_change::example.table_change_ui from ( 
    select * from example.invoice_changes(id=>id,hist_tables_detail=>false,hist_columns_insert=>true)
    union all
    select * from example.invoice_product_changes(invoice_id=>id)
    order by 1,2,3
  ) tc;  
$$;
select * from example.invoice_changes_ui(12);

