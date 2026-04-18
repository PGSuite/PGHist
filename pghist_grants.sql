grant usage on schema pghist to :roles;

grant execute on procedure pghist.hist_enable(name,name,name,name,name[]),pghist.hist_enable(name)  to :roles;
grant execute on procedure pghist.hist_disable(name,name),                pghist.hist_disable(name) to :roles;

grant execute on procedure pghist.hist_expression_row_desc         to :roles;
grant execute on function  pghist.hist_expression_row_desc_default to :roles;
grant execute on function  pghist.hist_expression_row_desc_current to :roles;

grant execute on procedure pghist.hist_expression_value_desc         to :roles;
grant execute on function  pghist.hist_expression_value_desc_default to :roles;
grant execute on function  pghist.hist_expression_value_desc_current to :roles;

