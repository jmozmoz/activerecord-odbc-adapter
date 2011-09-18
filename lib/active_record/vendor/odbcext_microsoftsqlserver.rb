#
#  $Id: odbcext_microsoftsqlserver.rb,v 1.3 2008/04/13 22:46:09 source Exp $
#
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

require 'faster_csv'

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
  # Optional methods
  #
  # These are supplied for a DBMS only if necessary.
  # ODBCAdapter tests for optional methods using Object#respond_to?

  # Pre action for ODBCAdapter#insert
  def pre_insert(sql, name, pk, id_value, sequence_name)
    @logger.unknown("ODBCAdapter#pre_insert>") if @trace
    @logger.unknown("args=[#{sql}|#{name}|#{pk}|#{id_value}|#{sequence_name}]") if @trace
    @iiTable = get_table_name(sql)
    @iiCol = get_autounique_column(@iiTable)
    @logger.unknown("@iiCol=#{@iiCol}>") if @trace
    @iiEnabled = false

    if @iiCol != nil
      if query_contains_autounique_col(sql, @iiCol)
        begin
          remove_null_sequence_value_from_sql(sql, @iiCol)
        rescue Exception => e
          raise ActiveRecordError, "IDENTITY_INSERT could not be turned on"
        end
      end
    end
  end

  # Post action for ODBCAdapter#insert
  def post_insert(sql, name, pk, id_value, sequence_name)
    if @iiEnabled
      begin
        @connection.do(enable_identity_insert(@iiTable, false))
      rescue Exception => e
        raise ActiveRecordError, "IDENTITY_INSERT could not be turned off"
      end
    end
  end

  # ------------------------------------------------------------------------
  # Method redefinitions
  #
  # DBMS specific methods which override the default implementation
  # provided by the ODBCAdapter core.
  #

  def quoted_date(value)
    @logger.unknown("ODBCAdapter#quoted_date (MS SQL)>") if @trace
    @logger.unknown("args=[#{value}]") if @trace
    # MS SQL DBTIME and DBDATE environment variables should be set to:
    # DBTIME=%d.%m.%Y %H:%M:%S
    # DBDATE=%d.%m.%Y
    if value.acts_like?(:time) # Time, DateTime
      %Q!'#{value.strftime("%Y%m%d %H:%M:%S")}'!
    else # Date
      %Q!'#{value.strftime("%Y%m%d")}'!
    end
  end

  def create_database(name)
    @logger.unknown("ODBCAdapter#create_database>") if @trace
    @logger.unknown("args=[#{name}]") if @trace
    execute "CREATE DATABASE #{name}"
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def drop_database(name)
    @logger.unknown("ODBCAdapter#drop_database>") if @trace
    @logger.unknown("args=[#{name}]") if @trace
    execute "DROP DATABASE #{name}"
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def rename_table(name, new_name)
    @logger.unknown("ODBCAdapter#rename_table>") if @trace
    execute "EXEC sp_rename '#{name}', '#{new_name}'"
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def remove_column(table_name, column_name)
    @logger.unknown("ODBCAdapter#remove_column>") if @trace
    # Remove default constraints first
    defaults = select_all "select def.name from sysobjects def, syscolumns col, sysobjects tab where col.cdefault = def.id and col.name = '#{column_name}' and tab.name = '#{table_name}' and col.id = tab.id"
    defaults.each {|constraint|
      execute "ALTER TABLE #{quote_table_name(table_name)} DROP CONSTRAINT #{constraint["name"]}"
    }
    execute "ALTER TABLE #{quote_table_name(table_name)} DROP COLUMN #{quote_column_name(column_name)}"
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def change_column(table_name, column_name, type, options = {})
    @logger.unknown("ODBCAdapter#change_column>") if @trace
    sql_commands = ["ALTER TABLE #{table_name} ALTER COLUMN #{column_name} #{type_to_sql(type, options[:limit], options[:precision], options[:scale])}"]
    if options_include_default?(options)
      # Remove default constraints first
      defaults = select_all "select def.name from sysobjects def, syscolumns col, sysobjects tab where col.cdefault = def.id and col.name = '#{column_name}' and tab.name = '#{table_name}' and col.id = tab.id"
      defaults.each {|constraint|
        execute "ALTER TABLE #{table_name} DROP CONSTRAINT #{constraint["name"]}"
      }
      sql_commands << "ALTER TABLE #{table_name} ADD CONSTRAINT DF_#{table_name}_#{column_name} DEFAULT #{quote(options[:default])} FOR #{column_name}"
    end
    sql_commands.each {|c|
      execute(c)
    }
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def change_column_default(table_name, column_name, default)
    @logger.unknown("ODBCAdapter#change_column_default>") if @trace
    # Remove default constraints first
    defaults = select_all "select def.name from sysobjects def, syscolumns col, sysobjects tab where col.cdefault = def.id and col.name = '#{column_name}' and tab.name = '#{table_name}' and col.id = tab.id"
    defaults.each {|constraint|
      execute "ALTER TABLE #{table_name} DROP CONSTRAINT #{constraint["name"]}"
    }
    execute "ALTER TABLE #{table_name} ADD CONSTRAINT DF_#{table_name}_#{column_name} DEFAULT #{quote(default)} FOR #{column_name}"
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def rename_column(table_name, column_name, new_column_name)
    @logger.unknown("ODBCAdapter#rename_column>") if @trace
    execute "EXEC sp_rename '#{table_name}.#{column_name}', '#{new_column_name}'"
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def remove_index(table_name, options = {})
    @logger.unknown("ODBCAdapter#remove_index>") if @trace
    execute "DROP INDEX #{table_name}.#{quote_column_name(index_name(table_name, options))}"
  rescue Exception => e
    @logger.unknown("exception=#{e}") if @trace
    raise
  end

  def tables(name = nil)
    # Hide system tables.
    super(name).delete_if {|t| t =~ /^sys/ }
  end

  def indexes(table_name, name = nil)
    # Hide primary key indexes.
    super(table_name, name).delete_if { |i| i.name =~ /^PK_/ }
  end

  # ------------------------------------------------------------------------
  # Private methods to support methods above
  #
  private

  def get_table_name(sql)
    @logger.unknown("ODBCAdapter#get_table_name>") if @trace
    @logger.unknown("sql=[#{sql}]") if @trace
    idQuoteChar = @dsInfo.info[ODBC::SQL_IDENTIFIER_QUOTE_CHAR]
    if sql =~ /^\s*insert\s+into\s+#{idQuoteChar}([^\(\s]+)#{idQuoteChar}\s*|^\s*update\s+#{idQuoteChar}([^\(\s]+)#{idQuoteChar}\s*/i
      $1
    elsif sql =~ /from\s+#{idQuoteChar}([^\(\s]+)#{idQuoteChar}\s*/i
      $1
    else
      nil
    end
  end

  def get_autounique_column(table_name)
    @logger.unknown("ODBCAdapter#get_autounique_column>") if @trace
    @logger.unknown("args=[#{table_name}]") if @trace
    @table_columns = {} unless @table_columns
    @table_columns[table_name] = columns(table_name) if @table_columns[table_name] == nil
    @table_columns[table_name].each do |col|
      return col.name if col.auto_unique?
    end

    return nil
  end

  def query_contains_autounique_col(sql, col)
    idQuoteChar = @dsInfo.info[ODBC::SQL_IDENTIFIER_QUOTE_CHAR]
    sql =~ /(\[#{col}\])|("#{col}")|(#{idQuoteChar}#{col}#{idQuoteChar})/
  end

  def enable_identity_insert(table_name, enable = true)
    if has_autounique_column(table_name)
      "SET IDENTITY_INSERT #{table_name} #{enable ? 'ON' : 'OFF'}"
    end
  end

  def has_autounique_column(table_name)
    !get_autounique_column(table_name).nil?
  end

  def remove_null_sequence_value_from_sql(sql, sequence_column)
    @logger.unknown("ODBCAdapter#remove_null_sequence_value_from_sql>") if @trace
    @logger.unknown("sql=[#{sql}|#{sequence_column}]") if @trace
    idQuoteChar = @dsInfo.info[ODBC::SQL_IDENTIFIER_QUOTE_CHAR]
    valQuoteChar = "'"
    sql =~ /(.*)\((.*)\) *VALUES *\((.*)\)(.*)/
    start = $1
    columns = FasterCSV.parse($2, :quote_char=>idQuoteChar, :col_sep=>', ').flatten
    values =  FasterCSV.parse($3, :quote_char=>valQuoteChar, :col_sep=>', ').flatten
    rest_str = $3
    rest = $4
    @logger.unknown("start=#{start}") if @trace
    @logger.unknown("columns=#{columns.join(', ')}") if @trace
    @logger.unknown("values=#{values.join(', ')}") if @trace
    @logger.unknown("rest=#{rest}") if @trace
    raise "Could not parse SQL string: #{sql}" if columns.length != values.length
    new_columns = []
    new_values = []
    columns.each_index do |i|
      @logger.unknown("columns[i]=#{columns[i]}") if @trace
      if (columns[i] != sequence_column) or (values[i] != "NULL")
        new_columns << quote_column_name(columns[i])
        values[i].gsub!(/#{valQuoteChar}/, "#{valQuoteChar}#{valQuoteChar}") # requote quote char
        values[i] = Regexp.escape(values[i]) # now escape it to use it in the regexp below
        rest_str =~ /^ *(#{valQuoteChar}*#{values[i]}[^,]*),* *.*/
        new_values << $1
        @logger.unknown("columns[i]=#{columns[i]}") if @trace
        @logger.unknown("$1=#{$1}") if @trace
      end
      if (columns[i] == sequence_column) and (values[i] != "NULL")
        @connection.do(enable_identity_insert(@iiTable, true))
        @iiEnabled = true
      end
      rest_str =~ /^ *#{valQuoteChar}*#{values[i]}[^,]*, *(.*)/
      rest_str = $1
      @logger.unknown("rest_str=#{rest_str}") if @trace
    end

    @logger.unknown("new_columns=#{new_columns.join(', ')}") if @trace
    @logger.unknown("new_values=#{new_values.join(', ')}") if @trace

    @sql_pre_insert = start + "(#{new_columns.join(', ')}) " +
                      "VALUES (#{new_values.join(', ')})" + rest
  end

end # module
