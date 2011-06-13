for adapter in %w( odbc )
  Rake::TestTask.new("test_#{adapter}") { |t|
    if adapter =~ /jdbc/
      t.libs << "test" << "test/connections/jdbc_#{adapter}"
    else
      t.libs << "test" << "test/connections/native_#{adapter}"
    end
    adapter_short = adapter == 'db2' ? adapter : adapter[/^[a-z]+/]
    t.test_files=Dir.glob( "test/cases/**/*_test{,_#{adapter_short}}.rb" ).sort
    t.verbose = true
  }

  namespace adapter do
    task :test => "test_#{adapter}"
  end
end

namespace :odbc do
  desc 'Build the ODBC test databases'
  task :build_databases do
    # FIXME Create databases activerecord_unittest and 
    # activerecord_unittest2 here
  end

  desc 'Drop the ODBC test databases'
  task :drop_databases do
    # FIXME Drop databases activerecord_unittest and 
    # activerecord_unittest2 here
  end

  desc 'Rebuild the ODBC test databases'
  task :rebuild_databases => [:drop_databases, :build_databases]
end

task :build_odbc_databases => 'odbc:build_databases'
task :drop_odbc_databases => 'odbc:drop_databases'
task :rebuild_odbc_databases => 'odbc:rebuild_databases'
