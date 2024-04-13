grant usage on schema pghist to :roles;

grant execute on procedure pghist.hist_enable(name,name,name,name),pghist.hist_enable(name)  to :roles;
grant execute on procedure pghist.hist_disable(name,name),         pghist.hist_disable(name) to :roles;

grant execute on procedure pghist.hist_expression_row_desc         to :roles;
grant execute on function  pghist.hist_expression_row_desc_default to :roles;
grant execute on function  pghist.hist_expression_row_desc_current to :roles;

grant execute on procedure pghist.hist_expression_value_desc         to :roles;
grant execute on function  pghist.hist_expression_value_desc_default to :roles;
grant execute on function  pghist.hist_expression_value_desc_current to :roles;

grant all on function pghist.hist_custom_operation_name  to :roles;
grant all on function pghist.hist_custom_db_user_name    to :roles;    
grant all on function pghist.hist_custom_app_user        to :roles;
grant all on function pghist.hist_custom_app_user_name   to :roles;   
grant all on function pghist.hist_custom_client_addr     to :roles;      
grant all on function pghist.hist_custom_client_hostname to :roles;      

