do $$
declare
  v_rec record;
begin
  if to_regnamespace('pghist') is null then	
    return;
  end if;	
  for v_rec in
    select schema,name from pghist.hist_table
  loop
    call pghist.hist_disable(v_rec.schema,v_rec.name);
  end loop;
end; $$;

drop extension if exists pghist;

drop event trigger if exists pghist_event_tg_ddl_command;
drop event trigger if exists pghist_event_tg_drop_table;

drop schema if exists pghist cascade;

