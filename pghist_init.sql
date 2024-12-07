create schema if not exists pghist;

create or replace function pghist.pghist_version() returns varchar language plpgsql as $$
begin
  return '24.4.7';
end; $$;

create table if not exists pghist.hist_transaction(
  id bigint primary key,
  xid bigint not null,  
  timestamp_start timestamptz not null,
  timestamp_commit timestamptz,
  application_name varchar,
  backend_pid integer not null,
  backend_start timestamptz not null,  
  db_user name not null,
  db_client_addr inet,
  db_client_hostname varchar,
  app_user name,
  app_client_addr inet,
  app_client_hostname varchar  
) with (fillfactor=90);
create sequence if not exists pghist.hist_transaction_seq as bigint increment 2;

create table if not exists pghist.hist_query(
  hash bigint primary key,
  text text not null  
);
  
create table if not exists pghist.hist_statement(
  id bigint primary key,
  transaction_id bigint not null, -- references pghist.hist_transaction(id) on delete cascade,
  timestamp timestamptz not null,
  operation varchar(16) not null check (operation in ('HIST_ENABLE','INSERT','UPDATE','DELETE','TRUNCATE')),
  query_hash bigint not null -- references pghist.hist_query(hash)   
);
create sequence if not exists pghist.hist_statement_seq as bigint increment 2;

create table if not exists pghist.hist_table(
  schema name,
  name name,
  primary key (schema,name),
  pkey name[] not null,
  master_table_schema name, 
  master_table_name name,
  master_table_fkey name[],  
  columns_excluded name[] not null,
  expression_row_desc varchar
);

create table if not exists pghist.hist_table_column(
  schema name,
  table_name name,
  foreign key (schema,table_name) references pghist.hist_table on delete cascade,
  column_name name,
  primary key (schema,table_name,column_name),
  expression_value_desc varchar  
);

create table if not exists pghist.hist_sql_log(
  id serial primary key,
  pghist_version varchar(8) not null default pghist.pghist_version(),
  transaction_id bigint not null references pghist.hist_transaction(id) on delete cascade,  
  schema name not null,
  table_name name not null,  
  sql_statement text not null
);

do $$ begin
  if to_regtype('pghist.hist_table_change') is null then
    create type pghist.hist_table_change as (
      statement_num bigint,
      row_num int,
      column_num int,
      timestamp timestamptz,
      operation varchar,
      operation_name varchar,
      column_name name,
      column_comment varchar,      
      value_old text,
      value_old_desc text,      
      value_new text,
      value_new_desc text,
      row_desc text,      
      db_user name,
      db_user_name varchar,
      app_user varchar,
      app_user_name varchar,
      schema name,
      table_name name,
      table_comment text     
    );  
  end if;
end $$;

create or replace function pghist.hist_transaction_fn_commit() returns trigger security definer language plpgsql as $$
begin
  update pghist.hist_transaction set timestamp_commit = clock_timestamp() where id=new.id;  
  return null;
end; $$;

do $$ begin
  if not exists (select from pg_trigger where tgrelid='pghist.hist_transaction'::regclass::oid and tgname='hist_transaction_tg_commit') then
    create constraint trigger hist_transaction_tg_commit after insert on pghist.hist_transaction deferrable initially deferred for each row execute procedure pghist.hist_transaction_fn_commit();  
  end if; 
end $$;

create or replace function pghist.hist_statement_id(operation varchar) returns bigint language plpgsql as $$
declare
  v_id bigint := reverse(nextval('pghist.hist_statement_seq')::varchar);
  v_transaction_id bigint = pghist.hist_transaction_id();
  v_query_text text = current_query();
  v_query_hash bigint := hashtextextended(v_query_text, 0);  
begin
  insert into pghist.hist_query(hash, text) values (v_query_hash, v_query_text) on conflict (hash) do nothing;
  insert into pghist.hist_statement(id, transaction_id, timestamp, operation, query_hash)
    values (v_id, v_transaction_id, clock_timestamp(), operation, v_query_hash);
  return v_id;
end; $$;

create or replace procedure pghist.hist_execute_sql(schema name, table_name name, sql_statement varchar) language plpgsql as $$
begin
  execute sql_statement;      
  insert into pghist.hist_sql_log(transaction_id,schema,table_name,sql_statement)
    values(pghist.hist_transaction_id(),schema,table_name,sql_statement);  
end; $$;

create or replace function pghist.hist_ident(ident name) returns name language plpgsql as $$
begin
  return case when left(ident,1)='"' then ident else lower(ident) end;
end; $$;

create or replace function pghist.hist_ident(prefix varchar, ident name, suffix varchar) returns name language plpgsql as $$
begin
  return case when left(ident,1)='"' then '"'||prefix||substr(ident,2,length(ident)-2)||suffix||'"' else prefix||lower(ident)||suffix end;
end; $$;

create or replace function pghist.hist_ident_in_str(ident text) returns name language plpgsql as $$
begin
  return replace(ident,'''','''''');
end; $$;

create or replace function pghist.hist_ident_table(prefix varchar, schema name, table_name name) returns varchar language plpgsql as $$
begin
  if left(schema,1)!='"' and left(table_name,1)!='"' then  	
  	return prefix||schema||'_'||table_name;
  end if;
  return 
    '"' || prefix || case when left(schema,1)='"' then substr(schema,2,length(schema)-2) else schema end ||
    '_' || case when left(table_name,1)='"' then substr(table_name,2,length(table_name)-2) else table_name end || '"';
end; $$;

create or replace procedure pghist.hist_objects_name(schema name, table_name name, inout hist_data_table name, inout hist_data_func_prefix name, inout trigger_prefix name, inout view_hist name, inout func_changes name, inout func_at_timestamp name) language plpgsql as $$
begin
  hist_data_table       := pghist.hist_ident_table('hist_data$', schema, table_name);
  hist_data_func_prefix := pghist.hist_ident('', hist_data_table, '$'); 
  hist_data_table       := 'pghist.'||hist_data_table;  
  --
  trigger_prefix    := pghist.hist_ident('hist_', table_name, '_tg_');
  view_hist         := pghist.hist_ident('',      table_name, '_hist');  
  func_changes      := pghist.hist_ident('',      table_name, '_changes');
  func_at_timestamp := pghist.hist_ident('',      table_name, '_at_timestamp');
end; $$;

create or replace function pghist.hist_exists(schema name, table_name name) returns boolean language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
begin
  return exists (select from pghist.hist_table where hist_table.schema=v_schema and hist_table.name=v_table_name);
end; $$;

create or replace function pghist.hist_columns_to_text(columns name[], expr text default 'col', delimiter varchar default ',') returns varchar language plpgsql as $$
declare
  v_text text;
begin
  if pg_catalog.array_length(columns, 1) is null then return ''; end if;	
  execute format('select string_agg(%s, ''%s'' order by pos) from unnest($1) with ordinality r(col,pos)', expr, delimiter )
    into v_text
    using columns;	
  return v_text;  
end; $$;

create or replace procedure pghist.hist_table_columns(table_oid oid, columns_excluded name[], inout columns_name name[], inout columns_type name[], inout columns_comment varchar[]) language plpgsql as $$
begin	
  select array_agg(quote_ident(attname) order by attnum),
         array_agg(case when tn.nspname!='pg_catalog' then quote_ident(tn.nspname)||'.' else '' end || quote_ident(typname) order by attnum), 
         array_agg(coalesce(quote_literal(col_description(attrelid,attnum)),'null') order by attnum)
    into columns_name, columns_type, columns_comment
    from pg_attribute a
    join pg_type t on t.oid=a.atttypid
    join pg_namespace tn on tn.oid=t.typnamespace
    where attrelid=table_oid and attnum>0 and not attisdropped and not quote_ident(attname)=any(columns_excluded);  
end; $$;

create or replace function pghist.hist_master_table_fkey(table_oid oid, master_table_schema name, master_table_name name) returns name[] language plpgsql as $$
declare
  v_fkey_out int[];
  v_fkey_in int[];
  v_master_pkey int[];
  v_master_table_oid oid;
  v_master_table_fkey name[];  
begin	
  if master_table_schema is null then 
    return null;
  end if;
  v_master_table_oid := (master_table_schema||'.'||master_table_name)::regclass::oid;
  select d.conkey,d.confkey,m.conkey master_pkey
    into v_fkey_out,v_fkey_in,v_master_pkey
    from pg_constraint d
    join pg_constraint m on m.conrelid=d.confrelid and m.contype='p'
    where d.contype='f' and d.conrelid=table_oid and d.confrelid=v_master_table_oid
    order by d.conkey[1]
    limit 1;
  if v_fkey_out is null then
    raise exception 'PGHIST-003 Not found foreign key on master table';
  end if;      
  return (
    select array_agg(quote_ident(a.attname) order by master_pkey.pos)
      from unnest(v_master_pkey) with ordinality master_pkey(num,pos)
      join pg_attribute a on a.attrelid=table_oid and a.attnum=v_fkey_out[array_position(v_fkey_in, master_pkey.num)]
  );    
end; $$;

create or replace function pghist.hist_columns_immutable(pkey name[], master_table_fkey name[]) returns name[] language plpgsql as $$
declare
  v_columns_immutable name[] := pkey;
  v_col name; 
begin
  if master_table_fkey is not null then	
    foreach v_col in array master_table_fkey loop
      if not v_col=any(pkey) then
        v_columns_immutable := v_columns_immutable||v_col;
      end if;
    end loop;
  end if;   
  return v_columns_immutable;  
end; $$;

create or replace procedure pghist.hist_enable(schema name, table_name name, master_table_schema name default null, master_table_name name default null, columns_excluded name[] default null) security definer language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
  v_table_oid oid := (v_schema||'.'||v_table_name)::regclass::oid;
  v_master_table_schema name := pghist.hist_ident(master_table_schema);
  v_master_table_name name := pghist.hist_ident(master_table_name);
  v_master_table_fkey name[] := pghist.hist_master_table_fkey(v_table_oid, v_master_table_schema, v_master_table_name);
  v_columns_excluded name[] := coalesce((select array_agg(pghist.hist_ident(ce)) from unnest(columns_excluded) ce), array[]::name[]);
  v_columns_name name[];
  v_columns_type name[];
  v_columns_comment varchar[];
  v_columns_pkey name[];
  v_columns_immutable name[];
  --
  v_hist_data_table name;
  v_hist_data_func_prefix name;
  v_trigger_prefix name;
  v_view_hist name;  
  v_func_changes name;
  v_func_at_timestamp name;
  --
  v_sql text;
  v_col name;
  v_operation varchar;
  v_newline char := E'\n';
begin
  if pghist.hist_exists(v_schema,v_table_name) then
  	perform pghist.hist_objects_refresh(v_schema,v_table_name);
  	return;
  end if;
  call pghist.hist_objects_name(v_schema, v_table_name, v_hist_data_table, v_hist_data_func_prefix, v_trigger_prefix, v_view_hist, v_func_changes, v_func_at_timestamp);
  call pghist.hist_table_columns(v_table_oid, v_columns_excluded, v_columns_name, v_columns_type, v_columns_comment);
  select array_agg(quote_ident(attname) order by col_pos)
    into v_columns_pkey
    from unnest( (select conkey from pg_constraint where conrelid=v_table_oid and contype='p') ) with ordinality c(col_num,col_pos)
    join pg_attribute a on attrelid=v_table_oid and attnum = col_num;
  if v_columns_pkey is null then
    raise exception 'PGHIST-002 Table does not have primary key';
  end if;   
  v_columns_immutable := pghist.hist_columns_immutable(v_columns_pkey, v_master_table_fkey);
  v_sql :=
    'create table '||v_hist_data_table||' ('||v_newline||
    '  hist_statement_id bigint not null, -- references pghist.hist_statement(id) on delete cascade,'||v_newline||
    '  hist_row_num int not null,'||v_newline||             
    '  hist_update_columns name[],'||v_newline;
  foreach v_col in array v_columns_immutable loop
    v_sql := v_sql||'  '||v_col||' '||v_columns_type[array_position(v_columns_name,v_col)]||' not null,'||v_newline;
  end loop; 
  foreach v_col in array v_columns_name loop
    if not v_col=any(v_columns_immutable) then
      v_sql := v_sql||'  '||pghist.hist_ident('',v_col,'_old')||' '||v_columns_type[array_position(v_columns_name,v_col)]||','||v_newline;
    end if;
  end loop;
  v_sql := v_sql||
   '  primary key ('||pghist.hist_columns_to_text(v_columns_pkey)||',hist_statement_id)'||v_newline||    
   ')';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'lock table '||v_hist_data_table||' in exclusive mode');
  v_sql :=
    'insert into '||v_hist_data_table||' (hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||')'||v_newline||   
    '  select '||pghist.hist_statement_id('HIST_ENABLE')||',row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||' from only '||v_schema||'.'||v_table_name;
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  if v_master_table_fkey is not null then
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create index on '||v_hist_data_table||'('||pghist.hist_columns_to_text(v_master_table_fkey)||')');
  end if;
  insert into pghist.hist_table(schema,name,pkey,master_table_schema,master_table_name,master_table_fkey,columns_excluded)
    values (v_schema,v_table_name,v_columns_pkey,v_master_table_schema,v_master_table_name,v_master_table_fkey,v_columns_excluded);
  perform pghist.hist_objects_refresh(v_schema, v_table_name);
  foreach v_operation in array array['insert','update','delete','truncate'] loop
    v_sql :=
      'create trigger '||pghist.hist_ident('',v_trigger_prefix,v_operation)||
      ' '||case when v_operation='truncate' then 'before' else 'after' end||' '||v_operation||
      ' on '||v_schema||'.'||v_table_name||
      case when v_operation='truncate' then '' else ' referencing' end||      
      case when v_operation in ('insert','update') then ' new table as rows_new' else '' end||
      case when v_operation in ('update','delete') then ' old table as rows_old' else '' end||
      case when v_operation='truncate' then '' else ' for each statement' end||
      ' execute procedure pghist.'||pghist.hist_ident('',v_hist_data_func_prefix,v_operation)||'();';   
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  end loop;   
  if v_master_table_schema is not null then 
    perform pghist.hist_objects_refresh(v_master_table_schema, v_master_table_name);
  end if; 
end; $$;

create or replace procedure pghist.hist_enable(table_name name) security definer language plpgsql as $$
declare
  v_schema name;
  v_table_name name;
begin
  select quote_ident(n.nspname),quote_ident(c.relname) 
    into v_schema,v_table_name 
    from pg_class c
    join pg_namespace n on n.oid=c.relnamespace
    where c.oid=table_name::regclass::oid;
  call pghist.hist_enable(v_schema, v_table_name);
end; $$;

create or replace procedure pghist.hist_parent_refresh(schema name, table_name name) language plpgsql as $$
declare
  v_parent_schema name;
  v_parent_table_name name;
begin
  select quote_ident(quote_ident(c.relnamespace::regnamespace::name)),quote_ident(c.relname)
    into v_parent_schema,v_parent_table_name
    from pg_inherits i
    join pg_class c on c.oid=i.inhparent
    where i.inhrelid=(select oid from pg_class where quote_ident(relnamespace::regnamespace::name)=schema and quote_ident(relname)=table_name);
  if v_parent_schema is not null then
    perform pghist.hist_objects_refresh(v_parent_schema,v_parent_table_name);    
  end if;
end; $$;

create or replace procedure pghist.hist_disable(schema name, table_name name) security definer language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
  v_table_oid oid := to_regclass(v_schema||'.'||v_table_name)::oid;
  v_master_table_schema name;
  v_master_table_name name;
  v_hist_data_table name;
  v_hist_data_func_prefix name;
  v_trigger_prefix name;
  v_view_hist name;
  v_func_changes name;
  v_func_at_timestamp name;  
  v_operation varchar;
begin
  select v_master_table_schema,master_table_name
    into v_master_table_schema,v_master_table_name
    from pghist.hist_table ht
    where ht.schema=v_schema and ht.name=v_table_name;
  call pghist.hist_objects_name(v_schema, v_table_name, v_hist_data_table, v_hist_data_func_prefix, v_trigger_prefix, v_view_hist, v_func_changes, v_func_at_timestamp);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop view if exists '||v_schema||'.'||v_view_hist); 
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_schema||'.'||v_func_changes);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_schema||'.'||v_func_at_timestamp);
  foreach v_operation in array array['insert','update','delete','truncate'] loop
    call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists pghist.'||pghist.hist_ident('',v_hist_data_func_prefix,v_operation)||' cascade');  
  end loop;   
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop table if exists '||v_hist_data_table||' cascade');
  delete from pghist.hist_table where hist_table.schema=v_schema and hist_table.name=v_table_name;
  call pghist.hist_parent_refresh(v_schema, v_table_name);
  if v_master_table_schema is not null then 
    perform pghist.hist_objects_refresh(v_master_table_schema, v_master_table_name);
  end if; 
end; $$;

create or replace procedure pghist.hist_disable(table_name name) security definer language plpgsql as $$
declare
  v_schema name;
  v_table_name name;
begin
  select quote_ident(n.nspname),quote_ident(c.relname) 
    into v_schema,v_table_name 
    from pg_class c
    join pg_namespace n on n.oid=c.relnamespace
    where c.oid=table_name::regclass::oid;
  call pghist.hist_disable(v_schema, v_table_name);
end; $$;

create or replace function pghist.hist_event_column_rename(schema name, table_name name, table_oid oid, column_name_old name, column_num int) returns void security definer language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
  v_column_name_old name := quote_ident(column_name_old); 
  v_column_name_new name := quote_ident((select attname from pg_attribute where attrelid=table_oid and attnum=column_num));
  v_hist_column_name_old name := pghist.hist_ident('',v_column_name_old,'_old'); 
  --
  v_hist_data_table name;
  v_hist_data_func_prefix name;  
  v_trigger_prefix name;
  v_view_hist name;  
  v_func_changes name;
  v_func_at_timestamp name;
begin
  if v_column_name_old is null or v_column_name_new is null or v_column_name_old=v_column_name_new then
    return;
  end if; 
  call pghist.hist_objects_name(v_schema, v_table_name, v_hist_data_table, v_hist_data_func_prefix, v_trigger_prefix, v_view_hist, v_func_changes, v_func_at_timestamp);
  if not exists (select from pg_attribute where attrelid=v_hist_data_table::regclass::oid and quote_ident(attname)=v_hist_column_name_old) then
    return;
  end if;
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop view if exists '||v_schema||'.'||v_view_hist);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_data_table||' rename column '||v_hist_column_name_old||' to '||pghist.hist_ident('',v_column_name_new,'_old'));
  call pghist.hist_execute_sql(v_schema, v_table_name, 'update '||v_hist_data_table||' set hist_update_columns=array_replace(hist_update_columns,'||quote_literal(v_column_name_old)||','||quote_literal(v_column_name_new)||') where '||quote_literal(v_column_name_old)||'=any(hist_update_columns)');
end;
$$;

create or replace function pghist.hist_event_ddl_command() returns event_trigger stable security definer language plpgsql as $$
declare
  v_table record;
begin
  for v_table in 	
    select t.schema,t.name  
      from pg_event_trigger_ddl_commands() e
	  join pg_class c on c.oid=e.objid	  
      join pghist.hist_table t on t.schema=quote_ident(e.schema_name) and t.name=quote_ident(c.relname)
      where e.object_type='table' 
  loop
    perform pghist.hist_objects_refresh(v_table.schema, v_table.name);
  end loop;
  for v_table in 	
    select t.schema,t.name,e.objid oid,a.attname column_name_old,e.objsubid column_num
      from pg_event_trigger_ddl_commands() e
	  join pg_class c on c.oid=e.objid     
	  join pg_attribute a on a.attrelid=e.objid and a.attnum=e.objsubid
      join pghist.hist_table t on t.schema=quote_ident(e.schema_name) and t.name=quote_ident(c.relname)
      where e.object_type='table column' 
  loop
    perform tmp_log_insert(v_table.column_name_old||' '||v_table.column_num); -- ***
    perform pghist.hist_event_column_rename(v_table.schema, v_table.name, v_table.oid, v_table.column_name_old, v_table.column_num);
    perform pghist.hist_objects_refresh(v_table.schema, v_table.name);
  end loop;
  for v_table in 	
    select t.schema,t.name 
     from pg_event_trigger_ddl_commands() e
       join pg_index i on i.indexrelid=e.objid
       join pg_class c on c.oid=i.indrelid
       join pghist.hist_table t on t.schema=quote_ident(e.schema_name) and t.name=quote_ident(c.relname)
       where e.object_type='index'
  loop
    perform pghist.hist_objects_refresh(v_table.schema, v_table.name);
  end loop;
end;
$$;

create or replace function pghist.hist_event_drop_table() returns event_trigger security definer language plpgsql as $$
declare
  v_table record;
begin 
  for v_table in 
    select t.schema,t.name 
     from pg_event_trigger_dropped_objects() o
     join pghist.hist_table t on o.object_type='table' and t.schema=quote_ident(o.schema_name) and t.name=quote_ident(o.object_name) 
  loop    
	call pghist.hist_disable(v_table.schema, v_table.name);
  end loop;	
end;
$$;

do $$ begin
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_ddl_command') then
    create event trigger pghist_event_tg_ddl_command on ddl_command_end when tag in ('ALTER TABLE','CREATE INDEX','COMMENT') execute procedure pghist.hist_event_ddl_command();
  end if;
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_drop_table') then
    create event trigger pghist_event_tg_drop_table on sql_drop when tag in ('DROP TABLE','DROP SCHEMA') execute function pghist.hist_event_drop_table();
  end if; 
end $$;


create or replace function pghist.hist_objects_refresh(schema name, table_name name) returns void security definer language plpgsql as $body$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
  v_table_oid oid := (v_schema||'.'||v_table_name)::regclass::oid;
  v_table_comment varchar := coalesce(quote_literal(col_description(v_table_oid, 0)),'null');
  v_table_owner name := (select relowner::regrole::name from pg_class where oid=v_table_oid);
  v_tables_inherited name[][]; 
  v_columns_name name[];
  v_columns_type name[];
  v_columns_pkey name[];
  v_columns_comment varchar[];
  v_columns_immutable name[];
  v_columns_excluded name[];
  v_columns_value name[];
  v_columns_value_old name[];  
  v_columns_expr_value_new_desc varchar[];
  v_columns_expr_value_old_desc varchar[];
  v_row_desc_expr varchar; 
  --
  v_hist_data_table name;
  v_hist_data_func_prefix name; 
  v_trigger_prefix name;
  v_view_hist name;  
  v_func_changes name;
  v_func_at_timestamp name;
  --
  v_sql text;
  v_sql_part text; 
  v_sql_condition text;
  v_sql_hist_to_row text;
  v_sql_hist_data_func name;
  v_col name;
  v_operation varchar; 
  v_expression varchar;
  v_type_convert varchar;
  v_temp_table_at_timestamp name; 
  v_i int; 
  v_rec record; 
  v_newline char := E'\n';
begin
  call pghist.hist_objects_name(v_schema, v_table_name, v_hist_data_table, v_hist_data_func_prefix, v_trigger_prefix, v_view_hist, v_func_changes, v_func_at_timestamp);
  select ht.pkey,pghist.hist_columns_immutable(ht.pkey,ht.master_table_fkey),ht.columns_excluded
    into v_columns_pkey,v_columns_immutable,v_columns_excluded
    from pghist.hist_table ht
    where ht.schema=v_schema and ht.name=v_table_name;
  call pghist.hist_table_columns(v_table_oid, v_columns_excluded, v_columns_name, v_columns_type, v_columns_comment);
  select array_agg(array[child_schema,child_table_name] order by child_schema,child_table_name)
    into v_tables_inherited
    from (
      select quote_ident(n.nspname) child_schema, quote_ident(c.relname) child_table_name 
        from pg_inherits i
        join pg_class c on c.oid=i.inhrelid
        join pg_namespace n on n.oid=c.relnamespace
        where i.inhparent = v_table_oid
    ) c
    join pghist.hist_table t on t.schema=child_schema and t.name=child_table_name;
  -- 
  for v_rec in
    select quote_ident(substring(attname,1,length(attname)-4)) column_name, quote_ident(attname) column_name_old  
	  from pg_attribute
	  where attrelid=v_hist_data_table::regclass::oid and not quote_ident(attname)=any(v_columns_immutable) and attnum>3 and not attisdropped
        and (quote_ident(attname),atttypid) not in (
          select pghist.hist_ident('',quote_ident(attname),'_old'),atttypid from pg_attribute where attrelid=v_table_oid and attnum>0 and not attisdropped and not quote_ident(attname)=any(v_columns_excluded)
        )
  loop
    call pghist.hist_execute_sql(v_schema, v_table_name, 'drop view if exists '||v_schema||'.'||v_view_hist);
    call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_data_table||' drop column '||v_rec.column_name_old);
    call pghist.hist_execute_sql(v_schema, v_table_name, 'update '||v_hist_data_table||' set hist_update_columns=array_remove(hist_update_columns,'||quote_literal(v_rec.column_name)||') where '||quote_literal(v_rec.column_name)||'=any(hist_update_columns)');
  end loop;
  for v_rec in   
    select quote_ident(attname) column_name, typname column_type, pg_get_expr(d.adbin,d.adrelid) default_value
      from pg_attribute a
	  join pg_type t on t.oid=a.atttypid
	  left join pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum 
	  where attrelid=v_table_oid and not quote_ident(attname)=any(v_columns_immutable) and attnum>0 and not attisdropped and not quote_ident(attname)=any(v_columns_excluded)
        and pghist.hist_ident('',quote_ident(attname),'_old') not in (select quote_ident(attname) from pg_attribute where attrelid=v_hist_data_table::regclass::oid and attnum>0 and not attisdropped)                   
      order by attnum          
  loop
    call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_data_table||' add column '||pghist.hist_ident('',v_rec.column_name,'_old')||' '||v_rec.column_type);
    if v_rec.default_value is not null then
      call pghist.hist_execute_sql(v_schema, v_table_name, 'lock table '||v_hist_data_table||' in exclusive mode');
      v_sql :=
        'insert into '||v_hist_data_table||' (hist_statement_id,hist_row_num,hist_update_columns,'||pghist.hist_columns_to_text(v_columns_immutable)||')'||v_newline||   
        '  select '||pghist.hist_statement_id('UPDATE')||',row_number() over (),array['||pg_catalog.quote_literal(v_rec.column_name)||'],'||pghist.hist_columns_to_text(v_columns_immutable)||' from only '||v_schema||'.'||v_table_name;
      call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);         
    end if; 
  end loop; 
  select array_agg(col),array_agg(pghist.hist_ident('',col,'_old')) into v_columns_value,v_columns_value_old from unnest(v_columns_name) col where not col = any(v_columns_immutable); 
  for v_i in 1..array_length(v_columns_name,1) loop
    v_expression := pghist.hist_expression_value_desc_current(v_schema, v_table_name, v_columns_name[v_i]);
    if v_expression is not null then
      v_sql_part := ' execute '||quote_literal('select ('||v_expression||')')||' into v_change.value_';
      v_columns_expr_value_new_desc[v_i] := v_sql_part||'new_desc using v_row.'||v_columns_name[v_i]||';';
      v_columns_expr_value_old_desc[v_i] := v_sql_part||'old_desc using v_row.'||v_columns_name[v_i]||';';     
    else 
      v_columns_expr_value_new_desc[v_i] := ' v_change.value_new_desc := v_change.value_new;';
      v_columns_expr_value_old_desc[v_i] := ' v_change.value_old_desc := v_change.value_old;';     
    end if;
  end loop;
  v_row_desc_expr := 'execute '||quote_literal('select ('||pghist.hist_expression_row_desc_current(v_schema, v_table_name)||')')||' into v_change.row_desc using v_row;';
  -- 
  v_sql_hist_data_func := 'pghist.'||pghist.hist_ident('',v_hist_data_func_prefix,'insert');
  v_sql := 
    'create or replace function '||v_sql_hist_data_func||'() returns trigger language plpgsql security definer as $$'||v_newline||
    'declare '||v_newline||
    '  v_statement_id bigint = pghist.hist_statement_id(tg_op);'||v_newline||
    'begin '||v_newline||
    '  insert into '||v_hist_data_table||'(hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||')'||v_newline||
    '    select v_statement_id,row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||v_newline||
    '      from rows_new;'||v_newline||
    '  return null;'||v_newline||
    'end;'||v_newline||
    '$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_sql_hist_data_func||' to '||v_table_owner);
  v_sql_hist_data_func := 'pghist.'||pghist.hist_ident('',v_hist_data_func_prefix,'update');
  v_sql := 
    'create or replace function '||v_sql_hist_data_func||'() returns trigger language plpgsql security definer as $$'||v_newline||
    'declare '||v_newline||
    '  v_statement_id bigint = pghist.hist_statement_id(tg_op);'||v_newline||
    'begin '||v_newline||    
    '  if exists ('||v_newline||
    '    select from rows_old o'||v_newline||
    '      left join rows_new n on '||pghist.hist_columns_to_text(v_columns_immutable,$$ 'o.'||col||'=n.'||col $$,' and ')||v_newline||
    '      where n.'||v_columns_pkey[1]||' is null'||v_newline||
    '  ) then '||v_newline||
    '    raise exception ''PGHIST-001 Update column(s) '||pghist.hist_columns_to_text(v_columns_immutable,$$ pghist.hist_ident_in_str(col) $$)||' of table '||pghist.hist_ident_in_str(v_schema)||'.'||pghist.hist_ident_in_str(v_table_name)||' is not allowed'';'||v_newline||
    '  end if;'||v_newline|| 
    '  insert into '||v_hist_data_table||'(hist_statement_id,hist_row_num,hist_update_columns,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_value_old,$$ ','||col $$,'')||')'||v_newline||
    '    select v_statement_id,'||v_newline||
    '           hist_row_num,'||v_newline||
    '           array[]::name[]'||v_newline;
  v_sql_part := pghist.hist_columns_to_text(v_columns_immutable,$$ '             o.'||col||',' $$,v_newline)||v_newline;  
  for v_i in 1..array_length(v_columns_value,1) loop
    v_col := v_columns_value[v_i];
    v_type_convert := case when v_columns_type[array_position(v_columns_name, v_col)] in ('json','_json','xml','_xml') then '::text' else '' end;
    v_sql_condition := '(o.'||v_col||v_type_convert||'!=n.'||v_col||v_type_convert||') or (o.'||v_col||' is null and n.'||v_col||' is not null) or (o.'||v_col||' is not null and n.'||v_col||' is null)';
    v_sql := v_sql||'             ||(case when '||v_sql_condition||' then array['||quote_literal(v_col)||'] end)'||case when v_i=array_length(v_columns_value,1) then ',' else '' end||v_newline;
    v_sql_part := v_sql_part||'           case when '||v_sql_condition||' then o.'||v_col||' end'||case when v_i<array_length(v_columns_value,1) then ',' else '' end||v_newline;
  end loop;
  v_sql := v_sql||v_sql_part||
    '     from (select row_number() over () hist_row_num,* from rows_old) o'||v_newline||
    '     join rows_new n on '||pghist.hist_columns_to_text(v_columns_pkey,$$ 'o.'||col||'=n.'||col $$,' and ')||';'||v_newline||
    '  return null;'||v_newline|| 
    'end;'||v_newline||
    '$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_sql_hist_data_func||' to '||v_table_owner);
  v_sql_hist_data_func := 'pghist.'||pghist.hist_ident('',v_hist_data_func_prefix,'delete'); 
  v_sql := 
    'create or replace function '||v_sql_hist_data_func||'() returns trigger language plpgsql security definer as $$'||v_newline||
    'declare '||v_newline||
    '  v_statement_id bigint = pghist.hist_statement_id(tg_op);'||v_newline||
    'begin '||v_newline||    
    '  insert into '||v_hist_data_table||'(hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_value_old, $$ ','||col $$,'')||')'||v_newline||
    '    select v_statement_id,row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_value,$$ ','||col $$,'')||v_newline||
    '      from rows_old;'||v_newline||
    '  return null;'||v_newline||
    '  return null;'||v_newline||
    'end;'||v_newline||
    '$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_sql_hist_data_func||' to '||v_table_owner);
  v_sql_hist_data_func := 'pghist.'||pghist.hist_ident('',v_hist_data_func_prefix,'truncate'); 
  v_sql :=
    'create or replace function '||v_sql_hist_data_func||'() returns trigger language plpgsql security definer as $$'||v_newline||
    'declare '||v_newline||
    '  v_statement_id bigint = pghist.hist_statement_id(tg_op);'||v_newline||
    'begin '||v_newline||
    '  insert into '||v_hist_data_table||'(hist_statement_id,hist_row_num,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_value_old,$$ ','||col $$,'')||')'||v_newline||
    '    select v_statement_id,row_number() over (),'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_value,$$ ','||col $$,'')||v_newline||
    '      from '||v_schema||'.'||v_table_name||';'||v_newline||
    '   return null;'||v_newline||
    'end;'||v_newline||
    '$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_sql_hist_data_func||' to '||v_table_owner); 
  --
  v_sql := 
    'create or replace view '||v_schema||'.'||v_view_hist||' as '||v_newline||
    '  select reverse(h.hist_statement_id::varchar)::bigint hist_statement_num,hist_row_num,s.timestamp hist_timestamp,s.operation hist_operation,h.hist_update_columns,t.db_user hist_db_user,t.app_user hist_app_user,t.application_name hist_application_name,q.text hist_query_text,h.hist_statement_id,'||v_newline||
    '	      '||pghist.hist_columns_to_text(v_columns_immutable,$$ 'h.'||col $$)||pghist.hist_columns_to_text(v_columns_value_old,$$ ',h.'||col $$,'')||v_newline||
    '    from '||v_hist_data_table||' h'||v_newline||
    '    join pghist.hist_statement s on s.id=h.hist_statement_id'||v_newline||
    '    join pghist.hist_query q on q.hash=s.query_hash'||v_newline||
    '    join pghist.hist_transaction t on t.id=s.transaction_id'||v_newline||
    '  order by 1,2';  
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant select on '||v_schema||'.'||v_view_hist||' to '||v_table_owner||' with grant option');
  --
  v_sql_hist_to_row := pghist.hist_columns_to_text(v_columns_immutable,$$ 'v_row.'||col||' := v_hist.'||col||';' $$,' ')||pghist.hist_columns_to_text(v_columns_value,$$ ' v_row.'||col||' := v_hist.'||pghist.hist_ident('',col,'_old')||';' $$,'');    
  v_sql := 'create or replace function '||v_schema||'.'||v_func_changes||'(';
  v_sql_part := 'where true''||'; 
  for v_i in 1..array_length(v_columns_immutable,1) loop
    v_col := v_columns_immutable[v_i];     
    v_sql := v_sql||v_col||' '||v_columns_type[array_position(v_columns_name,v_col)]||' default null, ';
    v_sql_part := v_sql_part||'case when '||v_col||' is not null then '' and d.'||pghist.hist_ident_in_str(v_col)||'=$'||v_i||''' else '''' end||';
  end loop; 
  v_sql_part := v_sql_part||'''';
  v_sql := v_sql||'hist_columns_immutable boolean default false, hist_columns_insert boolean default false, hist_tables_detail boolean default true, hist_tables_inherited boolean default true) returns setof pghist.hist_table_change language plpgsql security definer as $$'||v_newline||
    'declare'||v_newline||
    '  v_row '||v_schema||'.'||v_table_name||'%rowtype;'||v_newline||    
    '  v_change pghist.hist_table_change;'||v_newline||
    '  v_hist record;'||v_newline||
    '  v_cur_hist refcursor;'||v_newline||
    'begin'||v_newline||
    '  v_change.schema := '||quote_literal(v_schema)||';'||v_newline||
    '  v_change.table_name := '||quote_literal(v_table_name)||';'||v_newline||
    '  v_change.table_comment := '||v_table_comment||';'||v_newline||   
    '  open v_cur_hist for execute'||v_newline||
    '    ''select reverse(s.id::varchar)::bigint hist_statement_num,d.hist_row_num,s.timestamp hist_timestamp,s.operation hist_operation,t.db_user hist_db_user,t.app_user hist_app_user,d.hist_update_columns,'||v_newline||
    '            '||pghist.hist_columns_to_text(v_columns_immutable,$$ 'd.'||pghist.hist_ident_in_str(col) $$)||pghist.hist_columns_to_text(v_columns_value_old,$$ ',d.'||pghist.hist_ident_in_str(col) $$,'')||v_newline||
    '       from '||pghist.hist_ident_in_str(v_hist_data_table)||' d'||v_newline||
    '       join pghist.hist_statement s on s.id=d.hist_statement_id'||v_newline||
    '       join pghist.hist_transaction t on t.id=s.transaction_id'||v_newline||
    '       '||v_sql_part||v_newline||
    '     union all'||v_newline||
    '     select null,null,null,null,null,null,null,'||v_newline||
    '            '||pghist.hist_columns_to_text(v_columns_immutable,$$ pghist.hist_ident_in_str(col) $$)||pghist.hist_columns_to_text(v_columns_value,$$ ','||pghist.hist_ident_in_str(col) $$,'')||v_newline||
    '       from only '||pghist.hist_ident_in_str(v_schema)||'.'||pghist.hist_ident_in_str(v_table_name)||' d'||v_newline||
    '       '||v_sql_part||v_newline||
    '     order by '||pghist.hist_columns_to_text(v_columns_pkey,$$ pghist.hist_ident_in_str(col) $$)||',hist_statement_num desc'''||v_newline||
    '    using '||pghist.hist_columns_to_text(v_columns_immutable)||';'||v_newline||
    '  loop'||v_newline||
    '    fetch v_cur_hist into v_hist;'||v_newline||
    '    exit when not found;'||v_newline||
    '    if v_hist.hist_operation is null then'||v_newline||
    '      '||v_sql_hist_to_row||v_newline||
    '      continue;'||v_newline||   
    '    end if;'||v_newline||
    '    v_change.statement_num  := v_hist.hist_statement_num;'||v_newline||    
    '    v_change.row_num        := v_hist.hist_row_num;'||v_newline||    
    '    v_change.timestamp      := v_hist.hist_timestamp;'||v_newline||
    '    v_change.operation      := v_hist.hist_operation;'||v_newline||
    '    v_change.operation_name := '||pghist.hist_column_custom_function_current('operation_name')||'(v_change.operation);'||v_newline||
    '    v_change.db_user        := v_hist.hist_db_user;'||v_newline||
    '    v_change.db_user_name   := '||pghist.hist_column_custom_function_current('db_user_name')||'(v_change.db_user);'||v_newline||
    '    v_change.app_user       := v_hist.hist_app_user;'||v_newline||
    '    v_change.app_user_name  := '||pghist.hist_column_custom_function_current('app_user_name')||'(v_change.app_user);'||v_newline||
    '    if v_hist.hist_operation in (''HIST_ENABLE'',''INSERT'') then'||v_newline||
    '      v_change.value_old := null; v_change.value_old_desc := null;'||v_newline||
    '      '||v_row_desc_expr||v_newline||
    '      if hist_columns_insert then'||v_newline;   
  for v_i in 1..array_length(v_columns_name,1) loop
    v_col := v_columns_name[v_i];
    if v_col=any(v_columns_immutable) then 
      v_sql := v_sql||'        if hist_columns_immutable then'||v_newline;
    else    
      v_sql := v_sql||'        if v_row.'||v_col||' is not null then'||v_newline;
    end if;
    v_sql := v_sql||
      '          v_change.column_name := '||quote_literal(v_col)||'; v_change.column_num := '||v_i||'; v_change.column_comment := '||v_columns_comment[v_i]||';'||v_newline||
      '          v_change.value_new := v_row.'||v_col||';'||v_columns_expr_value_new_desc[v_i]||v_newline||
      '          return next v_change;'||v_newline||
      '        end if;'||v_newline;  
  end loop;
  v_sql := v_sql||
    '      else'||v_newline||
    '        v_change.column_name := null; v_change.column_num := null; v_change.column_comment := null;'||v_newline||
    '        v_change.value_new := null; v_change.value_new_desc := null;'||v_newline||    
    '        return next v_change;'||v_newline||    
    '      end if;'||v_newline||    
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||
    '    if v_hist.hist_operation = ''UPDATE'' then'||v_newline||
    '      '||v_row_desc_expr||v_newline||
    '      foreach v_change.column_name in array v_hist.hist_update_columns loop'||v_newline;
  foreach v_col in array v_columns_value loop
    v_i := pg_catalog.array_position(v_columns_name, v_col);
    v_sql := v_sql||
      '        if v_change.column_name='||quote_literal(v_col)||' then'||v_newline||
      '          v_change.column_name := '||quote_literal(v_col)||'; v_change.column_num := '||v_i||'; v_change.column_comment := '||v_columns_comment[v_i]||';'||v_newline||
      '          v_change.value_new := v_row.'||v_col||';'||v_columns_expr_value_new_desc[v_i]||v_newline||
      '          v_row.'||v_col||' := v_hist.'||pghist.hist_ident('',v_col,'_old')||';'||v_newline|| 
      '          v_change.value_old := v_row.'||v_col||';'||v_columns_expr_value_old_desc[v_i]||v_newline||      
      '          return next v_change;'||v_newline||
      '          continue;'||v_newline|| 
      '        end if;'||v_newline;
  end loop;
  v_sql := v_sql|| 
    '      end loop;'||v_newline||
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||
    '    if v_hist.hist_operation in (''DELETE'',''TRUNCATE'') then'||v_newline||
    '      v_change.value_new := null; v_change.value_new_desc := null;'||v_newline||
    '      '||v_sql_hist_to_row||v_newline||
    '      '||v_row_desc_expr||v_newline;
  for v_i in 1..array_length(v_columns_name,1) loop
    v_col := v_columns_name[v_i];
    if v_col=any(v_columns_immutable) then 
      v_sql := v_sql||'      if hist_columns_immutable then'||v_newline;
    else    
      v_sql := v_sql||'      if v_hist.'||pghist.hist_ident('',v_col,'_old')||' is not null then'||v_newline;
    end if;
    v_sql := v_sql||
      '        v_change.column_name := '||quote_literal(v_col)||'; v_change.column_num := '||v_i||'; v_change.column_comment := '||v_columns_comment[v_i]||';'||v_newline||
      '        v_change.value_old := v_row.'||v_col||';'||v_columns_expr_value_old_desc[v_i]||v_newline||      
      '        return next v_change;'||v_newline||
      '      end if;'||v_newline;  
  end loop;
  v_sql := v_sql||
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||    
    '  end loop;'||v_newline||
    '  close v_cur_hist;'||v_newline;
  if v_tables_inherited is not null then    
    v_sql := v_sql||'  if hist_tables_inherited then'||v_newline;
    for v_i in 1..array_length(v_tables_inherited, 1) loop
      v_sql := v_sql||
        '    for v_change in (select * from '||v_tables_inherited[v_i][1]||'.'||pghist.hist_ident('',v_tables_inherited[v_i][2],'_changes')||'(where_clause, where_param, hist_columns_immutable, hist_columns_insert, hist_tables_detail, hist_tables_inherited)) loop'||v_newline||
        '      return next v_change;'||v_newline||
        '    end loop;'||v_newline;
    end loop;   
    v_sql := v_sql||'  end if;'||v_newline;
  end if;
  if exists (select from pghist.hist_table td where td.master_table_schema=v_schema and td.master_table_name=v_table_name) then
    v_sql := v_sql||'  if hist_tables_detail then'||v_newline;
    for v_rec in
      select td.schema,td.name,td.master_table_fkey,pghist.hist_columns_immutable(td.pkey, td.master_table_fkey) columns_immutable from pghist.hist_table td where td.master_table_schema=v_schema and td.master_table_name=v_table_name
    loop
      v_sql := v_sql||
        '    for v_change in (select * from '||v_rec.schema||'.'||pghist.hist_ident('',v_rec.name,'_changes')||'('||
        (select string_agg(coalesce(v_columns_pkey[array_position(v_rec.master_table_fkey,col)],'null'), ', ') from unnest(v_rec.columns_immutable) col)||
        ', hist_columns_immutable, hist_columns_insert, hist_tables_detail, hist_tables_inherited)) loop'||v_newline||
        '      return next v_change;'||v_newline||
        '    end loop;'||v_newline;
    end loop;
    v_sql := v_sql||'  end if;'||v_newline;
  end if; 
  v_sql := v_sql||    
    'end;'||v_newline||
    '$$';   
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_schema||'.'||v_func_changes||' to '||v_table_owner||' with grant option');
  -- 
  v_temp_table_at_timestamp := pghist.hist_ident_table('', v_schema, v_func_at_timestamp);
  v_sql :=
    'create or replace function '||v_schema||'.'||v_func_at_timestamp||'(transaction_timestamp timestamptz default current_setting(''pghist.at_timestamp'')::timestamptz, tables_inherited boolean default true) returns setof '||v_schema||'.'||v_table_name||' security definer language plpgsql as $func$'||v_newline||
    'declare'||v_newline||
    '  v_row '||v_schema||'.'||v_table_name||'%rowtype;'||v_newline||
    '  v_hist record;'||v_newline||
    '  v_hist_not_found boolean;'||v_newline||
    '  v_cur_hist cursor for'||v_newline||
    '    select s.operation hist_operation,h.hist_statement_id,h.hist_update_columns,'||pghist.hist_columns_to_text(v_columns_immutable,'''h.''||col')||pghist.hist_columns_to_text(v_columns_value_old,$$ ',h.'||col $$,'')||v_newline||
    '      from '||v_hist_data_table||' h'||v_newline||
    '      join pghist.hist_statement s on s.id=h.hist_statement_id'||v_newline||
    '      join pghist.hist_transaction t on t.id=s.transaction_id and t.timestamp_commit>=transaction_timestamp'||v_newline||    
    '    union all'||v_newline||
    '    select null,null,null,'||pghist.hist_columns_to_text(v_columns_immutable)||pghist.hist_columns_to_text(v_columns_value,$$ ','||col $$,'')||v_newline||
    '      from only '||v_schema||'.'||v_table_name||v_newline||    
    '    order by '||pghist.hist_columns_to_text(v_columns_pkey)||',hist_statement_id desc;'||v_newline||    
    '  v_column name;'||v_newline||
    'begin'||v_newline||
    '  drop table if exists '||v_temp_table_at_timestamp||';'||v_newline||
    '  create temp table '||v_temp_table_at_timestamp||' as select * from '||v_schema||'.'||v_table_name||' limit 0;'||v_newline||
    '  open v_cur_hist;'||v_newline||
    '  loop'||v_newline||
    '    fetch v_cur_hist into v_hist;'||v_newline||
    '    v_hist_not_found := not found;'||v_newline||
    '    if (v_hist_not_found and v_row.'||v_columns_pkey[1]||' is not null) or ('||pghist.hist_columns_to_text(v_columns_pkey,$$ 'v_row.'||col||'!=v_hist.'||col $$,' or ')||') then'||v_newline||
    '      insert into '||v_temp_table_at_timestamp||' values (v_row.*);'||v_newline||
    '      v_row := null;'||v_newline||   
    '    end if;'||v_newline||
    '    exit when v_hist_not_found;'||v_newline||
    '    if v_hist.hist_operation is null or v_hist.hist_operation in (''DELETE'',''TRUNCATE'') then'||v_newline||
    '    '||v_sql_hist_to_row||v_newline||
    '      continue;'||v_newline||   
    '    end if;'||v_newline||
    '    if v_hist.hist_operation = ''UPDATE'' then'||v_newline||
    '      foreach v_column in array v_hist.hist_update_columns loop'||v_newline;
  foreach v_col in array v_columns_value loop
    v_sql := v_sql||
      '        if v_column='||quote_literal(v_col)||' then v_row.'||v_col||':=v_hist.'||pghist.hist_ident('',v_col,'_old')||'; continue; end if;'||v_newline;
  end loop;
  v_sql := v_sql||   
    '      end loop;'||v_newline||
    '      continue;'||v_newline|| 
    '    end if;'||v_newline||
    '    if v_hist.hist_operation in (''INSERT'',''HIST_ENABLE'') then'||v_newline||
    '      v_row := null;'||v_newline||
    '      continue;'||v_newline||   
    '    end if;'||v_newline||
    '  end loop;'||v_newline||
    '  close v_cur_hist;'||v_newline;
  if v_tables_inherited is not null then    
    v_sql := v_sql||'  if tables_inherited then'||v_newline;
    for v_i in 1..array_length(v_tables_inherited, 1) loop
      v_sql := v_sql||
        '    insert into '||v_temp_table_at_timestamp||v_newline||
        '      select '||pghist.hist_columns_to_text(v_columns_name)||v_newline||
        '         from '||v_tables_inherited[v_i][1]||'.'||pghist.hist_ident('',v_tables_inherited[v_i][2],'_at_timestamp')||'(transaction_timestamp, tables_inherited);'||v_newline;
    end loop;   
    v_sql := v_sql||'  end if;'||v_newline;
  end if; 
  select 
      string_agg( 
        '  create index on '||v_temp_table_at_timestamp||' ('|| 
        (select string_agg(quote_ident(attname), ',' order by col_pos)
           from unnest(indkey) with ordinality col(col_num,col_pos)
           join pg_attribute on attrelid=indrelid and attnum=col_num
        )||
  	    '); -- '||relname||v_newline
  	   , '' order by relname)    
  	into v_sql_part   
    from pg_index
    join pg_class on pg_class.oid=indexrelid
    where indrelid=v_table_oid and 0!=any(indkey);   
  v_sql := v_sql||v_sql_part||   
    '  for v_row in (select * from '||v_temp_table_at_timestamp||') loop'||v_newline||
    '    return next v_row;'||v_newline||    
    '  end loop;'||v_newline||
    'end;'||v_newline||
    '$func$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'grant execute on function '||v_schema||'.'||v_func_at_timestamp||' to '||v_table_owner||' with grant option');
  --
  call pghist.hist_parent_refresh(v_schema, v_table_name);
end; $body$;


create or replace procedure pghist.hist_expression_row_desc(schema name, table_name name, expression varchar) language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
begin
  update pghist.hist_table ht set expression_row_desc=expression where ht.schema=v_schema and ht.name=v_table_name;	
  call pghist.hist_enable(schema, table_name); 
end; $$;

create or replace function pghist.hist_expression_row_desc_default(schema name, table_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (pghist.hist_ident(schema)||'.'||pghist.hist_ident(table_name))::regclass::oid;
begin
  return (
    select quote_literal(coalesce(col_description(v_table_oid, 0), table_name)||' #')||'||'||string_agg('$1.'||quote_ident(attname), '||'',''||' order by col_pos) expr
      from unnest( (select conkey from pg_constraint where conrelid=v_table_oid and contype='p') ) with ordinality c(col_num,col_pos)
      join pg_attribute a on attrelid=v_table_oid and attnum = col_num
  );	
end; $$;

create or replace function pghist.hist_expression_row_desc_current(schema name, table_name name) returns varchar language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
begin
  return (select coalesce(expression_row_desc,pghist.hist_expression_row_desc_default(ht.schema,ht.name)) from pghist.hist_table ht where ht.schema=v_schema and ht.name=v_table_name);
end; $$;

create or replace procedure pghist.hist_expression_value_desc(schema name, table_name name, column_name name, expression varchar) language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
  v_column_name name := pghist.hist_ident(column_name);
begin
  insert into pghist.hist_table_column(schema,table_name,column_name,expression_value_desc)
    values (v_schema,v_table_name,v_column_name,expression)
    on conflict on constraint hist_table_column_pkey do
    update set expression_value_desc = expression;
  call pghist.hist_enable(v_schema, v_table_name); 
end; $$;

create or replace function pghist.hist_expression_value_desc_default(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (pghist.hist_ident(schema)||'.'||pghist.hist_ident(table_name))::regclass::oid;
  v_column_name name := pghist.hist_ident(column_name);
begin
  return (
    select 'select '||quote_ident(f_text.attname)||' from '||quote_ident(f_nsp.nspname)||'.'||quote_ident(f_tab.relname)||' where '||quote_ident(f_key.attname)||'=$1'  
      from pg_constraint c
      join pg_attribute ca on ca.attrelid=c.conrelid and ca.attnum=c.conkey[1] and quote_ident(ca.attname)=v_column_name 
      join pg_class f_tab on f_tab.oid=c.confrelid
      join pg_namespace f_nsp on f_nsp.oid=f_tab.relnamespace
      join pg_attribute f_key on f_key.attrelid=c.confrelid and f_key.attnum=c.confkey[1]
      join pg_attribute f_text on f_text.attrelid=c.confrelid and f_text.attnum!=f_key.attnum 
      join pg_type t on t.oid=f_text.atttypid and t.typcategory='S'
      where c.conrelid=v_table_oid and c.contype='f' and array_length(c.conkey,1)=1
      order by f_text.attnum
      limit 1
  );
end; $$;

create or replace function pghist.hist_expression_value_desc_current(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
declare
  v_schema name := pghist.hist_ident(schema);
  v_table_name name := pghist.hist_ident(table_name);
  v_column_name name := pghist.hist_ident(column_name);
begin
  return coalesce(
    (select expression_value_desc from pghist.hist_table_column htc where htc.schema=v_schema and htc.table_name=v_table_name and htc.column_name=v_column_name),
    pghist.hist_expression_value_desc_default(v_schema, v_table_name, v_column_name)
  );	
end; $$;

create or replace function pghist.hist_default_operation_name(operation varchar) returns varchar language plpgsql as $$
begin 
  return case
	when operation = 'HIST_ENABLE' then 'History start'
	when operation = 'INSERT'      then 'Creation'
	when operation = 'UPDATE'      then 'Modification'
	when operation = 'DELETE'      then 'Deletion'
	when operation = 'TRUNCATE'    then 'Cleaning'
  end;
end; $$;

create or replace function pghist.hist_default_db_user_name(db_user name)   returns varchar language plpgsql as $$ begin return db_user;                                        end; $$;
create or replace function pghist.hist_default_app_user()                   returns varchar language plpgsql as $$ begin return current_setting('app.user', true);              end; $$;      
create or replace function pghist.hist_default_app_user_name(app_user name) returns varchar language plpgsql as $$ begin return app_user;                                       end; $$;
create or replace function pghist.hist_default_app_client_addr()            returns inet    language plpgsql as $$ begin return current_setting('app.client_addr', true)::inet; end; $$;      
create or replace function pghist.hist_default_app_client_hostname()        returns varchar language plpgsql as $$ begin return current_setting('app.client_hostname', true);   end; $$;      

create table if not exists pghist.hist_column_custom_function(
  column_name name primary key check (column_name in ('operation_name','db_user_name','app_user','app_user_name','app_client_addr','app_client_hostname')),
  custom_function name not null
);

create or replace function pghist.hist_column_custom_function_current(column_name name) returns varchar language plpgsql as $$
declare
  v_column_name name := column_name;
begin
  return coalesce((select custom_function from pghist.hist_column_custom_function f where f.column_name=v_column_name), 'pghist.hist_default_'||v_column_name);
end; $$;

create or replace procedure pghist.hist_column_custom_function(column_name name, custom_function name) language plpgsql as $$
declare
  v_custom_function name := custom_function;
  v_rec_table record;
begin
  insert into pghist.hist_column_custom_function 
    values (column_name, v_custom_function)
    on conflict on constraint hist_column_custom_function_pkey do
    update set custom_function = v_custom_function;
  if column_name in ('app_user','app_client_addr','app_client_hostname') then    
    call pghist.hist_create_function_transaction_id();
  else 
    for v_rec_table in
      select schema,name from pghist.hist_table
    loop
      call pghist.hist_enable(v_rec_table.schema, v_rec_table.name);
    end loop;
  end if;   
end; $$;

create or replace procedure pghist.hist_create_function_transaction_id() security definer language plpgsql as $body$
begin
  execute $$
  
create or replace function pghist.hist_transaction_id() returns bigint language plpgsql as $func$
declare
  v_id_text varchar;
  v_id bigint;
begin
  v_id_text := current_setting('pghist.transaction_id', true);
  if v_id_text!='' then
    return v_id_text::bigint;
  end if; 
  v_id := reverse(nextval('pghist.hist_transaction_seq')::varchar);
  insert into pghist.hist_transaction(id, xid, timestamp_start, application_name, backend_pid, backend_start, db_user, db_client_addr, db_client_hostname, app_user, app_client_addr, app_client_hostname)
    select v_id, txid_current(), xact_start, application_name, pg_backend_pid(), backend_start, usename, client_addr, client_hostname, $$ 
           || pghist.hist_column_custom_function_current('app_user') || '(), ' || pghist.hist_column_custom_function_current('app_client_addr') || '(), ' || pghist.hist_column_custom_function_current('app_client_hostname') || '()' || $$ 
    from pg_stat_activity p where pid = pg_backend_pid();
  perform set_config('pghist.transaction_id', v_id::varchar, true);
  return v_id;
end; $func$;

  $$;
end; $body$;

call pghist.hist_create_function_transaction_id();


