
require("oltp_common")

function prepare_statements()
  prepare_for_each_table("delete_with_secondary")
end

function event()
  execute_delete_with_secondary()
end


