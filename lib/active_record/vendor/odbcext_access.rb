#  OpenLink ODBC Adapter for Ruby on Rails
#  Copyright (C) 2006 OpenLink Software
#
#  Permission is hereby granted, free of charge, to any person obtaining
#  a copy of this software and associated documentation files (the
#  "Software"), to deal in the Software without restriction, including
#  without limitation the rights to use, copy, modify, merge, publish,
#  distribute, sublicense, and/or sell copies of the Software, and to
#  permit persons to whom the Software is furnished to do so, subject
#  to the following conditions:
#
#  The above copyright notice and this permission notice shall be
#  included in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR
#  ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
#  CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
#  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

require 'active_record/connection_adapters/abstract_adapter'

module ODBCExt
  
  # ------------------------------------------------------------------------
  # Mandatory methods
  #
  
  # #last_insert_id must be implemented for any database which returns
  # false from #prefetch_primary_key?
  def last_insert_id(table, sequence_name, stmt = nil)
    @logger.unknown("ODBCAdapter#last_insert_id>") if @trace
    select_value("select @@IDENTITY", 'last_insert_id')
  end

  # ------------------------------------------------------------------------
  # Method redefinitions
  #
  # DBMS specific methods which override the default implementation 
  # provided by the ODBCAdapter core.

  def add_column_options!(name, options = {})
    @logger.unknown("ODBCAdapter#add_column_options overloaded>") if @trace
    @logger.unknown("name=#{name}") if @trace
    if options.has_key?(:default) then
      options.delete(:default) 
      # FIXME: Call ALTER TABLE via JetDB or something else to set default!
      # see e.g. http://goo.gl/9wqH9
    end
    super(name, options)
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def type_to_sql(type, limit = nil, precision = nil, scale = nil) # :nodoc:
    @logger.unknown("ODBCAdapter#type_to_sql overloaded>") if @trace
    @logger.unknown("args=[#{type}|#{limit}|#{precision}|#{scale}]") if @trace
    super(type, limit)
    # FIXME: Call ALTER TABLE via ADO or something else to set 
    # precision and scale! See e.g. http://goo.gl/CPI45
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end
end # module