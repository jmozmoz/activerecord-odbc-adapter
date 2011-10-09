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
require "csv"

module ODBCExt

  # ------------------------------------------------------------------------
  # Mandatory methods
  #

  # #last_insert_id must be implemented for any database which returns
  # false from #prefetch_primary_key?
  def last_insert_id(table, sequence_name, stmt = nil)
    @logger.unknown("ODBCAdapter#last_insert_id>") if @trace
    @logger.unknown("ODBCAdapter#last_insert_id args=[#{table}|#{sequence_name}|#{stmt}]") if @trace
    select_value("select max(#{quote_column_name(sequence_name)}) " +
                 "from #{quote_table_name(table)}", 'last_insert_id')
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
    @logger.unknown("@iiTable=#{@iiTable}") if @trace
    @iiCol = get_autounique_column(@iiTable)
    @logger.unknown("@iiCol=#{@iiCol}") if @trace

    if @iiCol != nil
      if query_contains_autounique_col(sql, @iiCol)
        begin
          remove_null_sequence_value_from_sql(sql, @iiCol)
#        rescue Exception => e
#          raise ActiveRecordError, "IDENTITY_INSERT could not be turned on"
        end
      end
    end
  end

  # Returns the default sequence name for a table.
  # Used for databases which don't support an autoincrementing column
  # type, but do support sequences.
  def default_sequence_name(table, column)
    @logger.unknown("ODBCAdapter#default_sequence_name>") if @trace
    @logger.unknown("args=[#{table}|#{column}]") if @trace
    get_autounique_column(table)
  end

  private


  def get_autounique_column(table_name)
    @logger.unknown("ODBCAdapter#get_autounique_column>") if @trace
    @logger.unknown("table_name=#{table_name}") if @trace
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

  def get_table_name(sql)
    @logger.unknown("ODBCAdapter#get_table_name>") if @trace
    @logger.unknown("sql=[#{sql}]") if @trace
    idQuoteChar = @dsInfo.info[ODBC::SQL_IDENTIFIER_QUOTE_CHAR]
    @logger.unknown("idQuoteChar=\"#{idQuoteChar}\"") if @trace
    if sql =~ /^\s*insert\s+into\s+#{idQuoteChar}([^\(\s]+)#{idQuoteChar}\s*|^\s*update\s+#{idQuoteChar}([^\(\s]+)#{idQuoteChar}\s*/i
      $1
    elsif sql =~ /from\s+#{idQuoteChar}([^\(\s]+)#{idQuoteChar}\s*/i
      $1
    else
      nil
    end
  end

  def remove_null_sequence_value_from_sql(sql, sequence_column)
    @logger.unknown("ODBCAdapter#remove_null_sequence_value_from_sql>") if @trace
    @logger.unknown("sql=[#{sql}|#{sequence_column}]") if @trace
    idQuoteChar = @dsInfo.info[ODBC::SQL_IDENTIFIER_QUOTE_CHAR]
    valQuoteChar = "'"
    sql.gsub!(/\r\n/, '\r\n') # escape CRLF
    sql =~ /(.*)\((.*)\) *VALUES *\((.*)\)(.*)/
    start = $1
    columns = CSV.parse($2, :quote_char=>idQuoteChar, :col_sep=>', ').flatten
    values =  CSV.parse($3, :quote_char=>valQuoteChar, :col_sep=>', ').flatten
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
        new_values << $1.gsub('\r\n', "\r\n") # insert CRLF again
        @logger.unknown("columns[i]=#{columns[i]}") if @trace
        @logger.unknown("$1=#{new_values.last}") if @trace
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
