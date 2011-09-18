#
#  $Id: connection.rb,v 1.6 2008/04/22 16:55:12 source Exp $
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

print "Using native ODBC\n"
require_dependency 'models/course'
require 'logger'
require 'win32ole'

RAILS_DEFAULT_LOGGER = Logger.new("debug_access_odbc.log")
#Logger level default is the lowest available, Logger::DEBUG
#RAILS_DEFAULT_LOGGER.level = Logger::WARN
#RAILS_DEFAULT_LOGGER.colorize_logging = false
ActiveRecord::Base.logger = RAILS_DEFAULT_LOGGER

BASE_DIR = FIXTURES_ROOT
msaccess_test_db  = "#{BASE_DIR}/rails_testdb1.mdb"
msaccess_test_db2 = "#{BASE_DIR}/rails_testdb2.mdb"

###########################################
# Using DSN-less connection with MS Access
# with possibly non-existing files

def make_connection(clazz, arunit, db_file)
  ActiveRecord::Base.configurations[arunit] =
    { :adapter => 'odbc',
      :conn_str => "Driver={Microsoft Access Driver (*.mdb)};DBQ=#{db_file}",
      :trace => true
    }
  unless File.exist?(db_file)
    puts "MS Access database not found at #{db_file}. Rebuilding it."
    conn = WIN32OLE.new("ADOX.Catalog")
    connection_string = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=#{db_file}"
    conn.Create(connection_string)
  end
  clazz.establish_connection(arunit)

end

make_connection(ActiveRecord::Base, 'arunit', msaccess_test_db)
make_connection(Course, 'arunit2', msaccess_test_db2)

###########################################
# Using DSN-less connection with MS Access
# with existing files

=begin
ActiveRecord::Base.configurations = {
  'arunit' => {
    :adapter  => "odbc",
    :conn_str=>"Driver={Microsoft Access Driver (*.mdb)};DBQ=rails_testdb1.mdb",
    :trace    => false
  },
 'arunit2' => {
    :adapter  => "odbc",
    :conn_str=>"Driver={Microsoft Access Driver (*.mdb)};DBQ=rails_testdb2.mdb",
    :trace    => false
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'
=end

###########################################
# Using DSN connection
=begin

ActiveRecord::Base.configurations = {
  'arunit' => {
    :adapter  => "odbc",
    :dsn      => "a609_test1",
    :username => "scott",
    :password => "tiger",
    :emulate_booleans => true,
    :trace    => true
  },
 'arunit2' => {
    :adapter  => "odbc",
    :dsn      => "a609_test2",
    :username => "scott",
    :password => "tiger",
    :emulate_booleans => true,
    :trace    => true
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'

ActiveRecord::Base.configurations = {
  'arunit' => {
    :adapter  => "odbc",
    :dsn      => "a609_ora10_alice_test1",
    :username => "scott",
    :password => "tiger",
    :emulate_booleans => true,
    :trace    => true
  },
 'arunit2' => {
    :adapter  => "odbc",
    :dsn      => "a609_ora10_alice_test2",
    :username => "scott",
    :password => "tiger",
    :emulate_booleans => true,
    :trace    => true
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'
=end

###########################################
# Using DSN-less connection

=begin
ActiveRecord::Base.configurations = {
  'arunit' => {
    :adapter  => "odbc",
    :conn_str => "Driver={OpenLink Lite for MySQL [6.0]};Database=rails_testdb1;Port=3306;UID=myuid;PWD=mypwd;"
    :emulate_booleans => true,
    :trace    => false
  },
 'arunit2' => {
    :adapter  => "odbc",
    :conn_str => "Driver={OpenLink Lite for MySQL [6.0]};Database=rails_testdb2;Port=3306;UID=myuid;PWD=mypwd;"
    :emulate_booleans => true,
    :trace    => false
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'
=end

###########################################
# Using DB2

=begin
ActiveRecord::Base.configurations = {
  'arunit' => {
  :adapter  => "odbc",
  :dsn	    => "a610_db2_alice_rails1",
  :username => "db2admin",
  :password => "db2admin",
  :trace    => true,
  :convert_numeric_literals => true
  },
 'arunit2' => {
    :adapter  => "odbc",
    :dsn      => "a610_db2_alice_rails2",
    :username => "db2admin",
    :password => "db2admin",
    :trace    => true,
    :convert_numeric_literals => true
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'
=end

###########################################
# Using Sybase 15

=begin
ActiveRecord::Base.configurations = {
  'arunit' => {
  :adapter  => "odbc",
  :dsn	    => "a609_syb15_trilby_testdb3",
  :username => "sa",
#  :password => "",
  :trace => true,
  :convert_numeric_literals => true
  },
 'arunit2' => {
  :adapter  => "odbc",
  :dsn	    => "a609_syb15_trilby_testdb4",
  :username => "sa",
#  :password => "",
  :trace => true,
  :convert_numeric_literals => true
  }
}

ActiveRecord::Base.establish_connection 'arunit'
Course.establish_connection 'arunit2'
=end

###########################################
puts "Using DSN: #{ActiveRecord::Base.configurations["arunit"][:dsn]}"
