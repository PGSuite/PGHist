create schema if not exists pghist;

create or replace function pghist.pghist_version() returns varchar language plpgsql as $$
begin
  return '23.3.10'; -- 2023.09.14 19:28:16
end; $$;

create table if not exists pghist.hist_transaction(
  id bigserial constraint hist_transaction_pkey primary key,
  xid bigint not null,  
  xact_start timestamptz not null,
  unique (xid, xact_start),
  application_name text,
  backend_pid integer not null,
  backend_start timestamptz not null,  
  db_user name not null,
  db_client_addr inet,
  db_client_hostname text,
  app_user name,
  app_client_addr text,
  app_client_hostname text  
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
  if to_regtype('pghist.table_change') is null then
    create type pghist.table_change as (
      column_name name,
      value_old text,
      value_old_desc text,      
      value_new text,
      value_new_desc text,
      row_desc text,      
      db_user varchar,
      app_user varchar,
      transaction_timestamp timestamptz,
      operation varchar,
      schema name,
      table_name name,
      table_comment text,     
      column_comment varchar,      
      column_pos int,
      row_pkey varchar[],
      transaction_id bigint,
      hist_id bigint
    );  
  end if;
end $$;

create sequence if not exists pghist.hist_id_seq;

create or replace function pghist.hist_transaction_id() returns bigint language plpgsql as $$
declare
  v_id bigint;
  v_id_text text;
begin
  v_id_text := coalesce(current_setting('pghist.transaction_id', true),'');
  if v_id_text!='' then
    return v_id_text::bigint;
  end if;  
  insert into pghist.hist_transaction(xid, xact_start, application_name, backend_pid, backend_start, db_user, db_client_addr, db_client_hostname, app_user, app_client_addr, app_client_hostname)
    select txid_current(), xact_start, application_name, pg_backend_pid(), backend_start, usename, client_addr, client_hostname, current_setting('app.user', true), current_setting('app.client_addr', true), current_setting('app.client_hostname', true)
    from pg_stat_activity p where pid = pg_backend_pid()
    returning id into v_id;
  v_id_text := set_config('pghist.transaction_id', v_id::text, true);
  return v_id;
end; $$;

create or replace procedure pghist.hist_execute_sql(schema name, table_name name, sql_statement varchar) language plpgsql security definer as $$
begin
  execute sql_statement;      
  insert into pghist.hist_sql_log(transaction_id,schema,table_name,sql_statement)
    values(pghist.hist_transaction_id(),schema,table_name,sql_statement);  
end; $$;

create or replace function pghist.hist_table_name(schema name, table_name name) returns varchar language plpgsql as $$
begin
  return 'hist_'||schema||'$'||table_name;
end; $$;

create or replace function pghist.hist_exists(schema name, table_name name) returns boolean language plpgsql as $$
begin
  return exists (select 1 from pg_tables where schemaname='pghist' and tablename=pghist.hist_table_name(schema,table_name));
end; $$;

create or replace function pghist.hist_column_desc_name(column_name name) returns varchar language plpgsql as $$
begin
  return lower(column_name)||'$hist_desc';
end; $$;


create or replace procedure pghist.hist_object_names(schema name, table_name name, inout hist_schema name, inout hist_table_name name, inout trigger_iud_function_name name, inout trigger_iud_name name, inout trigger_truncate_function_name name, inout trigger_truncate_name name, inout table_at_timestamp_name name, inout table_changes_name name) language plpgsql as $$
begin
  hist_schema := 'pghist';	
  hist_table_name := pghist.hist_table_name(schema,table_name);
  --
  trigger_iud_function_name := schema||'.'||hist_schema||'_'||table_name||'_fn_iud';
  trigger_iud_name := hist_schema||'_'||table_name||'_tg_iud';
  trigger_truncate_function_name := schema||'.'||hist_schema||'_'||table_name||'_fn_truncate';
  trigger_truncate_name := hist_schema||'_'||table_name||'_tg_truncate';
  --
  table_at_timestamp_name := schema||'.'||table_name||'_at_timestamp';
  table_changes_name := schema||'.'||table_name||'_changes';
end; $$;

create or replace procedure pghist.hist_enable(schema name, table_name name) language plpgsql as $body$
declare 
  v_schema name := lower(schema);
  v_table_name name := lower(table_name);
  v_table_oid oid := (v_schema||'.'||v_table_name)::regclass::oid;
  v_table_owner name;
  v_table_has_pkey boolean;
  v_table_row_desc_expr text; 
  v_children varchar[][];
  v_columns_name name[];
  v_columns_type name[];
  v_columns_pos int[];  
  v_columns_comment text[];
  v_columns_hist_desc name[];
  --
  v_hist_schema name;  
  v_hist_table_name name;
  v_hist_table_fix_columns text := 'hist_id,hist_transaction_id,hist_operation,hist_state,hist_pkey,hist_row_desc,';
  v_hist_table_fix_values text := 'v_hist_id,v_transaction_id,tg_op,'; 
  v_hist_table_fix_column_count int := 6; 
  v_hist_table_oid oid;
  v_hist_columns_ddl text[]; 
  v_hist_columns_ddl_desc text[];
  v_hist_columns_name text[];
  v_hist_row_values text[];
  v_hist_row_values_pkey text;
  --
  v_trigger_iud_function_name name;
  v_trigger_iud_name name;
  v_trigger_truncate_function_name name;
  v_trigger_truncate_name name;
  --
  v_table_at_timestamp_name name;
  v_table_at_timestamp_columns_pkey text;
  --
  v_table_changes_name name;
  --
  v_sql text; 
  v_newline char := chr(10);
  v_type_convert text; 
  rec record;
begin
  select relowner::regrole::name into v_table_owner from pg_class where oid=v_table_oid;
  call pghist.hist_object_names(v_schema, v_table_name, v_hist_schema, v_hist_table_name, v_trigger_iud_function_name, v_trigger_iud_name, v_trigger_truncate_function_name, v_trigger_truncate_name, v_table_at_timestamp_name, v_table_changes_name);  
  select array_agg(attname),
         array_agg(typname),
         array_agg(column_pos),         
         array_agg(quote_nullable(column_comment))
    into v_columns_name,
         v_columns_type,
         v_columns_pos,         
         v_columns_comment
    from ( 
      select attname, typname, row_number() over (order by attnum) column_pos, col_description(attrelid,attnum) column_comment    
        from pg_attribute a
        join pg_type t on t.oid=a.atttypid 
        where attrelid=v_table_oid and attnum>0
        order by attnum
    ) a;
  v_table_has_pkey := exists (select 1 from pg_index where indrelid = v_table_oid and indisprimary);
  if v_table_has_pkey then  
    select 'array['||string_agg('row.'||attname||'::varchar',',' order by array_position(i.indkey, a.attnum))||']',
           string_agg('h.'||attname,',' order by array_position(i.indkey, a.attnum))
      into v_hist_row_values_pkey, 
           v_table_at_timestamp_columns_pkey
      from pg_index i 
      join pg_attribute a on a.attrelid=i.indrelid and a.attnum = any(i.indkey)  
      where indrelid = v_table_oid and indisprimary;
  else     
     v_hist_row_values_pkey := 'null';
     v_table_at_timestamp_columns_pkey := null;
  end if;  
  select array_agg(array[c.relnamespace::regnamespace::name,c.relname] order by c.relnamespace::regnamespace::name,c.relname)
    into v_children
    from pg_inherits i
    join pg_class c on c.oid=i.inhrelid
    where inhparent = v_table_oid;
  --
  select oid into v_hist_table_oid from pg_class where relnamespace::regnamespace::name=v_hist_schema and relname=v_hist_table_name;
  if v_hist_table_oid is null then
    with att as (
      select attname,attnum,typname 
	    from pg_attribute a
	    join pg_type t on t.oid=a.atttypid 
        where attrelid=v_table_oid and attnum>0
	) 
    select array_agg(attname||' '||typname order by attnum),
           array_agg('comment on column '||v_hist_schema||'.'||v_hist_table_name||'.'||attname||' is '||quote_literal(desc_expr) order by attnum),
		   array_agg(attname order by attnum),
           array_agg(coalesce('('||desc_expr||')','row.'||attname) order by attnum)           
           into v_hist_columns_ddl,v_hist_columns_ddl_desc,v_hist_columns_name,v_hist_row_values
      from (
        select attname,typname,attnum,null desc_expr from att
        union all
	    select * from (
          select pghist.hist_column_desc_name(attname),'text',attnum+0.1,pghist.hist_column_desc_expression_default(v_schema,v_table_name,attname) desc_expr from att
	    ) columns_expr where desc_expr is not null     
	  ) columns;	  
    v_sql := 'create table '||v_hist_schema||'.'||v_hist_table_name||' ('||v_newline;  
    v_sql := v_sql||'  hist_id bigint,'||v_newline;
    v_sql := v_sql||'  hist_transaction_id bigint not null references pghist.hist_transaction(id) on delete cascade,'||v_newline;
    v_sql := v_sql||'  hist_operation varchar(11) not null check (hist_operation in (''HIST_CREATE'',''INSERT'',''UPDATE'',''DELETE'',''TRUNCATE'')),'||v_newline;
    v_sql := v_sql||'  hist_state varchar(3) not null check (hist_state in (''OLD'',''NEW'')),'||v_newline;
    v_sql := v_sql||'  primary key (hist_id,hist_state),'||v_newline;
    v_sql := v_sql||'  hist_pkey varchar[],'||v_newline;  
    v_sql := v_sql||'  hist_row_desc text,'||v_newline;   
    v_sql := v_sql||'  '||array_to_string(v_hist_columns_ddl,',')||v_newline;
    v_sql := v_sql||')';
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create index on '||v_hist_schema||'.'||v_hist_table_name||'(hist_transaction_id)');
    v_table_row_desc_expr := pghist.hist_row_desc_expression_default(v_schema, v_table_name);
	if v_table_row_desc_expr is not null then   
      call pghist.hist_execute_sql(schema, table_name, 'comment on table '||v_hist_schema||'.'||v_hist_table_name||' is '||quote_literal(v_table_row_desc_expr));
	else
	  v_table_row_desc_expr := 'null';
	end if;
	for rec in (select * from unnest(v_hist_columns_ddl_desc) ddl_desc where ddl_desc is not null) loop
   	  call pghist.hist_execute_sql(v_schema, v_table_name, rec.ddl_desc);
  	end loop;    
    call pghist.hist_execute_sql(v_schema, v_table_name, 'lock table '||v_hist_schema||'.'||v_hist_table_name||' in exclusive mode');
    v_sql := 'insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||')'; 
    v_sql := v_sql||'  select nextval(''pghist.hist_id_seq''),'||pghist.hist_transaction_id()||',''HIST_CREATE'',''NEW'','||v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,',')||' from only '||v_schema||'.'||v_table_name||' row';
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);   
    call pghist.hist_execute_sql(v_schema, v_table_name, 'grant select on '||v_hist_schema||'.hist_transaction,'||v_hist_schema||'.'||v_hist_table_name||' to '||v_table_owner);
    v_hist_table_oid := (v_hist_schema||'.'||v_hist_table_name)::regclass::oid;
  else
    for rec in
      with cols as (select attname,atttypid from pg_attribute where attrelid=v_table_oid and attnum>0 and not attisdropped)
	  select 'alter table '||v_hist_schema||'.'||v_hist_table_name||' drop column '||attname sql_drop_column
	    from pg_attribute a
	    where attrelid=v_hist_table_oid and attnum>v_hist_table_fix_column_count and not attisdropped
          and (attname,atttypid) not in (select * from cols)
          and attname not in (select pghist.hist_column_desc_name(attname) from cols) 
    loop
      call pghist.hist_execute_sql(v_schema, v_table_name, rec.sql_drop_column);
    end loop;
    for rec in   
      select attname column_name, typname column_type, pghist.hist_column_desc_expression_default(v_schema,v_table_name,attname) desc_expr, pghist.hist_column_desc_name(attname) column_desc_name
	    from pg_attribute a
	    join pg_type t on t.oid=a.atttypid	    
	    where attrelid=v_table_oid and attnum>0 and not attisdropped
          and attname not in (select attname from pg_attribute where attrelid=v_hist_table_oid and attnum>0 and not attisdropped)                   
        order by attnum          
    loop
      call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_schema||'.'||v_hist_table_name||' add column '||rec.column_name||' '||rec.column_type);
      if rec.desc_expr is not null then
        call pghist.hist_execute_sql(v_schema, v_table_name, 'alter table '||v_hist_schema||'.'||v_hist_table_name||' add column '||rec.column_desc_name||' text');
        call pghist.hist_execute_sql(v_schema, v_table_name, 'comment on column '||v_hist_schema||'.'||v_hist_table_name||'.'||rec.column_desc_name||' is '||quote_literal(rec.desc_expr));       
      end if;
    end loop;
    select coalesce(max(description),'null') into v_table_row_desc_expr from pg_description where objoid=v_hist_table_oid and objsubid=0;
    select array_agg(attname order by attnum),
           array_agg(case when attname like '%$hist_desc' then '('||col_description(v_hist_table_oid,attnum)||')' else 'row.'||attname end order by attnum)           
           into v_hist_columns_name,v_hist_row_values
      from pg_attribute a
      where attrelid=v_hist_table_oid and attnum>v_hist_table_fix_column_count;
  end if;
  select array_agg((select attname from pg_attribute where attrelid=v_hist_table_oid and attname=column_name||'$hist_desc'))
         into v_columns_hist_desc
    from unnest(v_columns_name) column_name;
  for rec in
    select 'drop index '||v_hist_schema||'.'||c.relname sql_drop_index 
      from pg_index i
      join pg_class c on c.oid=i.indexrelid
      where indrelid = v_hist_table_oid      
        and indkey not in (select indkey from pg_index where indrelid=v_table_oid)
        and (select min(unnest) from (select unnest(indkey)) indkey)>v_hist_table_fix_column_count
        and (select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a where a.attrelid=i.indrelid and a.attnum = any(i.indkey)) not in  
              (select (
                 select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a  where a.attrelid=i.indrelid and a.attnum = any(i.indkey)
              ) from pg_index i where indrelid = v_table_oid)
  loop
    call pghist.hist_execute_sql(schema, table_name, rec.sql_drop_index);
  end loop;
  for rec in 
    select 'create index on '||v_hist_schema||'.'||v_hist_table_name||'('||
           (select string_agg(attname,',' order by array_position(i.indkey, a.attnum)) from pg_attribute a where a.attrelid=i.indrelid and a.attnum = any(i.indkey))||
           ')' sql_create_index 
      from pg_index i
      where indrelid=v_table_oid
        and (select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a where a.attrelid=i.indrelid and a.attnum = any(i.indkey)) not in  
              (select (
                 select array_agg(attname order by array_position(i.indkey, a.attnum)) from pg_attribute a  where a.attrelid=i.indrelid and a.attnum = any(i.indkey)
              ) from pg_index i where indrelid = v_hist_table_oid)
  loop
    call pghist.hist_execute_sql(v_schema, v_table_name, rec.sql_create_index);
  end loop;
  --
  v_sql := 'create or replace function '||v_trigger_iud_function_name||'() returns trigger language plpgsql security definer as $$'||v_newline;
  v_sql := v_sql||'declare '||v_newline;
  v_sql := v_sql||'    v_transaction_id bigint;'||v_newline;
  v_sql := v_sql||'    v_hist_id bigint;'||v_newline;
  v_sql := v_sql||'begin '||v_newline;
  v_sql := v_sql||'    v_transaction_id := '||v_hist_schema||'.hist_transaction_id();'||v_newline;
  v_sql := v_sql||'    v_hist_id := nextval(''pghist.hist_id_seq'');'||v_newline;
  v_sql := v_sql||'    if (tg_op in (''INSERT'',''UPDATE'')) then'||v_newline;
  v_sql := v_sql||'        insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||') values ('||v_hist_table_fix_values||'''NEW'','||regexp_replace(v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,','),'row\.','new.','gi')||');'||v_newline;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    if (tg_op in (''UPDATE'',''DELETE'')) then'||v_newline;
  v_sql := v_sql||'        insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||') values ('||v_hist_table_fix_values||'''OLD'','||regexp_replace(v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,','),'row\.','old.','gi')||');'||v_newline;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    return null;'||v_newline;
  v_sql := v_sql||'end;'||v_newline;
  v_sql := v_sql||'$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql); 
  if not exists (select 1 from pg_trigger where tgrelid=v_table_oid and tgname=v_trigger_iud_name) then
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create trigger '||v_trigger_iud_name||' after insert or update or delete on '||schema||'.'||table_name||' for each row execute procedure '||v_trigger_iud_function_name||'();');
  end if;
  v_sql := 'create or replace function '||v_trigger_truncate_function_name||'() returns trigger language plpgsql security definer as $$'||v_newline;
  v_sql := v_sql||'declare '||v_newline;
  v_sql := v_sql||'  v_transaction_id bigint;'||v_newline;
  v_sql := v_sql||'begin '||v_newline;
  v_sql := v_sql||'  v_transaction_id := pghist.hist_transaction_id();'||v_newline;
  v_sql := v_sql||'  insert into '||v_hist_schema||'.'||v_hist_table_name||' ('||v_hist_table_fix_columns||array_to_string(v_hist_columns_name,',')||')'||v_newline; 
  v_sql := v_sql||'    select nextval(''pghist.hist_id_seq''),v_transaction_id,''TRUNCATE'',''OLD'','||v_hist_row_values_pkey||',('||v_table_row_desc_expr||'),'||array_to_string(v_hist_row_values,',')||' from only '||v_schema||'.'||v_table_name||' row;'||v_newline;
  v_sql := v_sql||'  return null;'||v_newline;
  v_sql := v_sql||'end;'||v_newline;
  v_sql := v_sql||'$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
  if not exists (select 1 from pg_trigger where tgrelid=v_table_oid and tgname=v_trigger_truncate_name) then
    call pghist.hist_execute_sql(v_schema, v_table_name, 'create trigger '||v_trigger_truncate_name||' before truncate on '||schema||'.'||table_name||' execute procedure '||v_trigger_truncate_function_name||'();');
  end if;
  --
  if v_table_has_pkey then  
    v_sql := 'create or replace function '||v_table_at_timestamp_name||'(transaction_start timestamptz, cascade boolean default true) returns setof '||v_schema||'.'||v_table_name||' language plpgsql security definer as $$'||v_newline;
    v_sql := v_sql||'declare'||v_newline;
    v_sql := v_sql||'  rec '||v_schema||'.'||v_table_name||'%rowtype;'||v_newline;
    v_sql := v_sql||'begin'||v_newline;
    v_sql := v_sql||'  for rec in '||v_newline;
    v_sql := v_sql||'    select '||array_to_string(v_columns_name,',')||' from ('||v_newline;
    v_sql := v_sql||'      select row_number() over (partition by '||v_table_at_timestamp_columns_pkey||' order by hist_id desc,hist_state) hist_row_number,h.*'||v_newline;
    v_sql := v_sql||'         from '||v_hist_schema||'.'||v_hist_table_name||' h'||v_newline;
    v_sql := v_sql||'         join pghist.hist_transaction t on t.id=h.hist_transaction_id and t.xact_start<transaction_start'||v_newline;      
    v_sql := v_sql||'    ) h where hist_row_number=1 and hist_state=''NEW'''||v_newline;
    v_sql := v_sql||'    loop'||v_newline;   
    v_sql := v_sql||'      return next rec;'||v_newline;
    v_sql := v_sql||'  end loop;'||v_newline;
    if v_children is not null then    
      v_sql := v_sql||'  if cascade then'||v_newline;
      for i in 1..array_length(v_children, 1) loop
        v_sql := v_sql||'    for rec in select '||array_to_string(v_columns_name,',')||' from '||v_children[i][1]||'.'||v_children[i][2]||'_at_timestamp(transaction_start, true) loop'||v_newline;
        v_sql := v_sql||'      return next rec;'||v_newline;
        v_sql := v_sql||'    end loop;'||v_newline;
      end loop;   
      v_sql := v_sql||'  end if;'||v_newline;
    end if; 
    v_sql := v_sql||'  return;'||v_newline;   
    v_sql := v_sql||'end;'||v_newline;   
    v_sql := v_sql||'$$';  
    call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);
    call pghist.hist_execute_sql(v_schema, v_table_name, 'alter function '||v_table_at_timestamp_name||' owner to '||v_table_owner);   
  else
    call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_table_at_timestamp_name||'(timestamptz, boolean)');
  end if;
  --
  v_sql := 'create or replace function '||v_table_changes_name||'(where_clause text default null, where_param anyelement default null::varchar, cascade boolean default true) returns setof pghist.table_change language plpgsql security definer as $$'||v_newline;
  v_sql := v_sql||'declare'||v_newline;
  v_sql := v_sql||'  column_comments varchar[] := array['||array_to_string(v_columns_comment,',')||'];'||v_newline;
  v_sql := v_sql||'  rec_change pghist.table_change;'||v_newline;
  v_sql := v_sql||'  rec_hist record;'||v_newline;
  v_sql := v_sql||'  rec_hist_update_old record;'||v_newline;
  v_sql := v_sql||'  cur_hist refcursor;'||v_newline;
  v_sql := v_sql||'  v_query text;'||v_newline; 
  v_sql := v_sql||'begin'||v_newline;
  v_sql := v_sql||'  v_query := ''select hist_row_desc,t.db_user hist_db_user,t.app_user hist_app_user,t.xact_start hist_transaction_timestamp,h.*'';'||v_newline;
  v_sql := v_sql||'  v_query := v_query||''  from '||v_hist_schema||'.'||v_hist_table_name||' h'';'||v_newline;
  v_sql := v_sql||'  v_query := v_query||''  join '||v_hist_schema||'.hist_transaction t on t.id=h.hist_transaction_id'';'||v_newline; 
  v_sql := v_sql||'  v_query := v_query||coalesce(''  where hist_id in (select hist_id from '||v_hist_schema||'.'||v_hist_table_name||' row where ''||where_clause||'')'','''');'||v_newline;
  v_sql := v_sql||'  v_query := v_query||''  order by hist_id,hist_state'';'||v_newline; 
  v_sql := v_sql||'  open cur_hist for execute v_query using where_param;'||v_newline;
  v_sql := v_sql||'  rec_change.schema := '''||v_schema||''';'||v_newline;
  v_sql := v_sql||'  rec_change.table_name := '''||v_table_name||''';'||v_newline;
  v_sql := v_sql||'  rec_change.table_comment := '||quote_nullable(obj_description(v_table_oid))||';'||v_newline; 
  v_sql := v_sql||'  loop'||v_newline;
  v_sql := v_sql||'    fetch cur_hist into rec_hist;'||v_newline;
  v_sql := v_sql||'    exit when not found;'||v_newline;
  v_sql := v_sql||'    rec_change.row_desc := rec_hist.hist_row_desc;'||v_newline; 
  v_sql := v_sql||'    rec_change.db_user := rec_hist.hist_db_user;'||v_newline; 
  v_sql := v_sql||'    rec_change.app_user := rec_hist.hist_app_user;'||v_newline;
  v_sql := v_sql||'    rec_change.transaction_timestamp := rec_hist.hist_transaction_timestamp;'||v_newline;
  v_sql := v_sql||'    rec_change.operation := rec_hist.hist_operation;'||v_newline;
  v_sql := v_sql||'    rec_change.row_pkey := rec_hist.hist_pkey;'||v_newline;
  v_sql := v_sql||'    rec_change.transaction_id  := rec_hist.hist_transaction_id ;'||v_newline; 
  v_sql := v_sql||'    rec_change.hist_id  := rec_hist.hist_id ;'||v_newline;
  v_sql := v_sql||'    if rec_hist.hist_operation = ''INSERT'' then'||v_newline;
  v_sql := v_sql||'      rec_change.value_old := null;'||v_newline; 
  v_sql := v_sql||'      rec_change.value_old_desc := null;'||v_newline; 
  for i in 1..array_length(v_columns_name, 1) loop
    v_sql := v_sql||'      if rec_hist.'||v_columns_name[i]||' is not null then rec_change.column_name := '''||v_columns_name[i]||'''; rec_change.column_pos := '||v_columns_pos[i]||'; rec_change.column_comment := column_comments['||i||']; rec_change.value_new := rec_hist.'||v_columns_name[i]||'; rec_change.value_new_desc := '||coalesce('rec_hist.'||v_columns_hist_desc[i],'null')||'; return next rec_change; end if;'||v_newline;
  end loop;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    if rec_hist.hist_operation = ''UPDATE'' then'||v_newline;
  v_sql := v_sql||'      fetch cur_hist into rec_hist_update_old;'||v_newline;
  v_sql := v_sql||'      exit when not found;'||v_newline;
  for i in 1..array_length(v_columns_name, 1) loop
    v_type_convert := case when v_columns_type[i]='json' then '::text' else '' end;
    v_sql := v_sql||'      if (rec_hist.'||v_columns_name[i]||v_type_convert||' != rec_hist_update_old.'||v_columns_name[i]||v_type_convert||') or (rec_hist.'||v_columns_name[i]||' is not null and rec_hist_update_old.'||v_columns_name[i]||' is null) or (rec_hist.'||v_columns_name[i]||' is null and rec_hist_update_old.'||v_columns_name[i]||' is not null) then rec_change.column_name := '''||v_columns_name[i]||'''; rec_change.column_pos := '||v_columns_pos[i]||'; rec_change.column_comment := column_comments['||i||']; rec_change.value_old := rec_hist_update_old.'||v_columns_name[i]||'; rec_change.value_old_desc := '||coalesce('rec_hist_update_old.'||v_columns_hist_desc[i],'null')||'; rec_change.value_new := rec_hist.'||v_columns_name[i]||'; rec_change.value_new_desc := '||coalesce('rec_hist.'||v_columns_hist_desc[i],'null')||'; return next rec_change; end if;'||v_newline;
  end loop;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'    if rec_hist.hist_operation in (''DELETE'',''TRUNCATE'') then'||v_newline;
  v_sql := v_sql||'      rec_change.value_new := null;'||v_newline;
  v_sql := v_sql||'      rec_change.value_new_desc := null;'||v_newline; 
  for i in 1..array_length(v_columns_name, 1) loop
    v_sql := v_sql||'      if rec_hist.'||v_columns_name[i]||' is not null then rec_change.column_name := '''||v_columns_name[i]||'''; rec_change.column_pos := '||v_columns_pos[i]||'; rec_change.column_comment := column_comments['||i||']; rec_change.value_old := rec_hist.'||v_columns_name[i]||'; rec_change.value_old_desc := '||coalesce('rec_hist.'||v_columns_hist_desc[i],'null')||'; return next rec_change; end if;'||v_newline;
  end loop;
  v_sql := v_sql||'    end if;'||v_newline;
  v_sql := v_sql||'  end loop;'||v_newline;
  v_sql := v_sql||'  close cur_hist;'||v_newline;
  if v_children is not null then    
    v_sql := v_sql||'  if cascade then'||v_newline;
    for i in 1..array_length(v_children, 1) loop
      v_sql := v_sql||'    for rec_change in select * from '||v_children[i][1]||'.'||v_children[i][2]||'_changes(where_clause, where_param, true) where column_name in ('''||array_to_string(v_columns_name,''',''')||''') loop'||v_newline;     
      v_sql := v_sql||'      return next rec_change;'||v_newline;
       v_sql := v_sql||'    end loop;'||v_newline;
    end loop;   
    v_sql := v_sql||'  end if;'||v_newline;
  end if; 
  v_sql := v_sql||'end;'||v_newline;
  v_sql := v_sql||'$$';
  call pghist.hist_execute_sql(v_schema, v_table_name, v_sql);  
  call pghist.hist_execute_sql(v_schema, v_table_name, 'alter function '||v_table_changes_name||' owner to '||v_table_owner);
end; $body$;

create or replace procedure pghist.hist_enable(table_name name) language plpgsql as $$
begin
  call pghist.hist_enable(current_schema(), table_name);
end; $$;

create or replace procedure pghist.hist_row_desc_expression(schema name, table_name name, expression varchar) language plpgsql as $$
begin
  call pghist.hist_execute_sql(schema, table_name, 'comment on table pghist.'||pghist.hist_table_name(schema,table_name)||' is '||coalesce(quote_literal(expression),'null'));
  call pghist.hist_enable(schema, table_name); 
end; $$;

create or replace function pghist.hist_row_desc_expression_default(schema name, table_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (schema||'.'||table_name)::regclass::oid;
begin
  if not exists (select 1 from pg_index where indrelid = v_table_oid and indisprimary) then 
    return null;
  end if;
  return (
    select quote_literal(coalesce(description, table_name)||' #')||'||'||pk.expr from (
      select string_agg('row.'||a.attname, '||'',''||' order by a.attnum) expr
        from pg_index i 
        join pg_attribute a on a.attrelid=i.indrelid and a.attnum = any(i.indkey)
        where indrelid = v_table_oid and indisprimary
      ) pk
      left join pg_description d on d.objoid=v_table_oid and d.objsubid=0
  );
end; $$;

create or replace function pghist.hist_row_desc_expression_current(schema name, table_name name) returns varchar language plpgsql as $$
begin
  return col_description(('pghist.'||pghist.hist_table_name(schema,table_name))::regclass::oid,0);	
end; $$;

create or replace procedure pghist.hist_column_desc_expression(schema name, table_name name, column_name name, expression varchar) language plpgsql as $$
declare
  v_hist_table name := 'pghist.'||pghist.hist_table_name(schema,table_name);
  v_hist_column_desc name := pghist.hist_column_desc_name(column_name);
begin
  call pghist.hist_execute_sql(schema, table_name, 'alter table '||v_hist_table||' add column if not exists '||v_hist_column_desc||' text');	
  call pghist.hist_execute_sql(schema, table_name, 'comment on column '||v_hist_table||'.'||v_hist_column_desc||' is '||coalesce(quote_literal(expression),'null'));
  call pghist.hist_enable(schema, table_name); 
end; $$;

create or replace function pghist.hist_column_desc_expression_default(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
declare
  v_table_oid oid := (schema||'.'||table_name)::regclass::oid;
  v_column_name name := lower(column_name);
begin
  return (
    select 'select '||f_text.attname||' from '||f_tab.relnamespace::regnamespace::name||'.'||f_tab.relname||' where '||f_key.attname||'=row.'||v_column_name  
      from pg_constraint c
      join pg_attribute ca on  ca.attrelid=c.conrelid and ca.attnum=c.conkey[1] and ca.attname=v_column_name 
      join pg_class f_tab on f_tab.oid=c.confrelid
      join pg_attribute f_key on f_key.attrelid=c.confrelid and f_key.attnum=c.confkey[1]
      join pg_attribute f_text on f_text.attrelid=c.confrelid and f_text.attnum!=f_key.attnum 
      join pg_type t on t.oid=f_text.atttypid and t.typcategory='S'
      where c.conrelid=v_table_oid and c.contype='f' and array_length(c.conkey,1)=1
      order by f_text.attnum
      limit 1
  );
end; $$;

create or replace function pghist.hist_column_desc_expression_current(schema name, table_name name, column_name name) returns varchar language plpgsql as $$
begin
  return ( select col_description(attrelid,attnum) from pg_attribute where attrelid=('pghist.'||pghist.hist_table_name(schema,table_name))::regclass::oid and attname=pghist.hist_column_desc_name(column_name) );	
end; $$;

create or replace procedure pghist.hist_disable(schema name, table_name name) language plpgsql as $$
declare
  v_schema name := lower(schema);
  v_table_name name := lower(table_name);
  v_hist_schema name;  
  v_hist_table_name name;
  v_trigger_iud_function_name name;
  v_trigger_iud_name name;
  v_trigger_truncate_function_name name;
  v_trigger_truncate_name name;
  v_table_at_timestamp_name name;
  v_table_changes_name name;
begin
  call pghist.hist_object_names(v_schema, v_table_name, v_hist_schema, v_hist_table_name, v_trigger_iud_function_name, v_trigger_iud_name, v_trigger_truncate_function_name, v_trigger_truncate_name, v_table_at_timestamp_name, v_table_changes_name);
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_table_at_timestamp_name||'(timestamptz, boolean)');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_table_changes_name||'(boolean, text, text, anyelement, anyelement, anyelement)');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_trigger_iud_function_name||' cascade');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop function if exists '||v_trigger_truncate_function_name||' cascade');
  call pghist.hist_execute_sql(v_schema, v_table_name, 'drop table if exists '||v_hist_schema||'.'||v_hist_table_name||' cascade');  
end; $$;

create or replace procedure pghist.hist_disable(table_name name) language plpgsql as $$
begin
  call pghist.hist_disable(current_schema(), table_name);
end; $$;

create or replace function pghist.event_fn_ddl_command() returns event_trigger security definer as $$
declare
  rec record;
begin
  for rec in select * from ( 
    select schema_name,relname table_name
      from pg_event_trigger_ddl_commands() e
      join pg_class c on c.oid=e.objid
      where object_type in ('table','table column')
    ) t where pghist.hist_exists(schema_name,table_name) 
  loop
    call pghist.hist_enable(rec.schema_name, rec.table_name);
  end loop;
  for rec in select * from ( 
    select schema_name schema_name,relname table_name
      from pg_event_trigger_ddl_commands() e
      join pg_index i on i.indexrelid=e.objid
      join pg_class c on c.oid=i.indrelid
      where object_type in ('index')
    ) t where pghist.hist_exists(schema_name,table_name) 
  loop
    call pghist.hist_enable(rec.schema_name, rec.table_name);
  end loop;	
end;
$$ language plpgsql;

create or replace function pghist.event_fn_drop_table() returns event_trigger security definer as $$
declare
  rec record;
begin  	
  for rec in 
    select schema_name,object_name table_name 
     from pg_event_trigger_dropped_objects()
     where object_type='table' and pghist.hist_exists(schema_name,object_name)
  loop
    call pghist.hist_disable(rec.schema_name, rec.table_name);
  end loop;	
end;
$$ language plpgsql;

do $$ begin
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_ddl_command') then
    create event trigger pghist_event_tg_ddl_command on ddl_command_end when tag in ('ALTER TABLE','CREATE INDEX','COMMENT') execute procedure pghist.event_fn_ddl_command();
  end if;
  if not exists (select 1 from pg_event_trigger where evtname='pghist_event_tg_drop_table') then
    create event trigger pghist_event_tg_drop_table on sql_drop when tag in ('DROP TABLE','DROP SCHEMA') execute function pghist.event_fn_drop_table();
  end if; 
end $$;
