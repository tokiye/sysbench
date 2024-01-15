


require("oltp_common")

-- cmdline:

sysbench.cmdline.options.secondary_start = 
    {"Secondary index begin. Must be positive", -1}
sysbench.cmdline.options.secondary_end = 
    {"Must be positive And larger than where secondary start", -1}
sysbench.cmdline.options.delta =
    {"Use for make range for delete statement", 1000}


-- create table: rewrite create table function
-- compare with the one in oltp_common, only change the usage of secondary
function create_table(drv, con, table_num)
   assert(sysbench.opt.secondary_start >= 0, "to run this test, secondary_start must be selected and be positive" .. "")
   assert(sysbench.opt.secondary_end >= 0, "to run this test, secondary_end must be selected and be positive" .. "")
   assert(sysbench.opt.delta > 0, "to run this test, delta must larger than 0" .. "")
   assert(sysbench.opt.secondary_end - sysbench.opt.secondary_start >= sysbench.opt.delta, "the size of (start,end) must larget than delta" .. "")	
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query

   if sysbench.opt.secondary then
     id_index_def = "KEY xid"
   else
     id_index_def = "PRIMARY KEY"
   end

   if drv:name() == "mysql"
   then
      if sysbench.opt.auto_inc then
         id_def = "INTEGER NOT NULL AUTO_INCREMENT"
      else
         id_def = "INTEGER NOT NULL"
      end
      engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
   elseif drv:name() == "pgsql"
   then
      if not sysbench.opt.auto_inc then
         id_def = "INTEGER NOT NULL"
      elseif pgsql_variant == 'redshift' then
        id_def = "INTEGER IDENTITY(1,1)"
      else
        id_def = "SERIAL"
      end
   else
      error("Unsupported database driver:" .. drv:name())
   end

   print(string.format("Creating table 'sbtest%d'...", table_num))

   query = string.format([[
CREATE TABLE sbtest%d(
  id %s,
  k INTEGER DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL,
  pad CHAR(60) DEFAULT '' NOT NULL,
  %s (id)
) %s %s]],
      table_num, id_def, id_index_def, engine_def,
      sysbench.opt.create_table_options)

   con:query(query)

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into 'sbtest%d'",
                          sysbench.opt.table_size, table_num))
   end

   if sysbench.opt.auto_inc then
      query = "INSERT INTO sbtest" .. table_num .. "(k, c, pad) VALUES"
   else
      query = "INSERT INTO sbtest" .. table_num .. "(id, k, c, pad) VALUES"
   end

   con:bulk_insert_init(query)

   local c_val
   local pad_val

   for i = 1, sysbench.opt.table_size do

      c_val = get_c_value()
      pad_val = get_pad_value()
      -- here the range of secondary we change
      if (sysbench.opt.auto_inc) then
         query = string.format("(%d, '%s', '%s')",
                               sysbench.rand.default(sysbench.opt.secondary_start, sysbench.opt.secondary_end),
                               c_val, pad_val)
      else
         query = string.format("(%d, %d, '%s', '%s')",
                               i,
                               sysbench.rand.default(sysbench.opt.secondary_start, sysbench.opt.secondary_end),
                               c_val, pad_val)
      end

      con:bulk_insert_next(query)
   end

   con:bulk_insert_done()

   -- we force to use secondary in this test
   --if sysbench.opt.create_secondary then
      print(string.format("Creating a secondary index on 'sbtest%d'...",
                          table_num))
      con:query(string.format("CREATE INDEX k_%d ON sbtest%d(k)",
                              table_num, table_num))
   --end
end



-- prepare for stmt
function prepare_for_each_table()
   for t = 1, sysbench.opt.tables do
      stmt[t].delete_use_secondary = con:prepare(string.format("DELETE /*+ MRR(sbtest%u) */ FROM sbtest%u WHERE k BETWEEN ? AND ?",t ,t))

      param[t].delete_use_secondary = {}
      param[t].delete_use_secondary[1] = stmt[t].delete_use_secondary:bind_create(sysbench.sql.type.INT)
      param[t].delete_use_secondary[2] = stmt[t].delete_use_secondary:bind_create(sysbench.sql.type.INT)

      stmt[t].delete_use_secondary:bind_param(unpack(param[t].delete_use_secondary))
   end
end


function prepare_statements()
  prepare_for_each_table()
end


function event()
   local iTable = sysbench.rand.uniform(1, sysbench.opt.tables)
   local iSecond = sysbench.rand.uniform(sysbench.opt.secondary_start, sysbench.opt.secondary_end - sysbench.opt.delta)
   
   param[iTable].delete_use_secondary[1]:set(iSecond)
   param[iTable].delete_use_secondary[2]:set(iSecond + sysbench.opt.delta)

   stmt[iTable].delete_use_secondary:execute()
end




