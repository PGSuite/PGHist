do $$
declare
  rec record;
begin
  if not exists (select 1 from pg_namespace where nspname='pghist') then 
    return;
  end if;	
  for rec in 
    select schemaname,tablename
      from pg_tables
      where pghist.hist_exists(schemaname::varchar,tablename::varchar)
  loop
    call pghist.hist_disable(rec.schemaname::varchar,rec.tablename::varchar);
  end loop;
end; $$;

drop event trigger if exists pghist_event_tg_ddl_command;
drop event trigger if exists pghist_event_tg_drop_table;

drop schema if exists pghist cascade;

