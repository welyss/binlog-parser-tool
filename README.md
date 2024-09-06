# binlog-parser-tool
A tool of MySQL binlog parser for print insert, update, delete query with variable filter, base on https://github.com/osheroff/mysql-binlog-connector-java
```
Install:
mvn clean package
[INFO] Building jar: C:\Users\xxxx\git\binlog-parser-tool\target\binlog-parser-tool-1.0.0-jar-with-dependencies.jar
[INFO] BUILD SUCCESS

Quick Start:
java -jar C:\Users\xxxx\git\binlog-parser-tool\target\binlog-parser-tool-1.0.0-jar-with-dependencies.jar -e
usage: binlog-parser-tool [Options] [BinlogFileName]
Options:
 -a,--ask-pass               Prompt for a password when connecting to
                             MySQL
 -c,--charset <arg>          Set character set for String.
 -d,--database <arg>         Filter: Database name
 -e,--help                   Show help
 -h,--host <arg>             Connect to host
 -j,--start-position <arg>   Start reading the binlog at position <arg>
 -k,--stop-position <arg>    Stop reading the binlog at position <arg>
 -m,--start-datetime <arg>   Start reading the binlog at datetime <arg>,
                             you should probably use quotes for your shell
                             to set it properly, format: [yyyy-MM-dd
                             HH:mm:ss], for example: -m "2004-12-25
                             11:25:56"
 -n,--stop-datetime <arg>    Stop reading the binlog at datetime <arg>,
                             you should probably use quotes for your shell
                             to set it properly, format: [yyyy-MM-dd
                             HH:mm:ss], for example: -n "2004-12-25
                             11:25:56"
 -P,--port <arg>             Port number to use for connection
 -p,--password <arg>         Password to use when connecting
 -s,--show-position          Show position
 -t,--table <arg>            Filter: Table name
 -u,--user <arg>             User for login if not current user
 -w,--where <arg>            Filter: Condition in where for filtering
                             data, Format: @column_index=value, For
                             example: @1=100
Sample:
  binlog-parser-tool /tmp/binlogs/mysql-bin.001130
```

Use Case:
```
# parser binlog file: mysql-bin.002142 from server->host:localhost,port:3307,user:test,ask for password start at binlog file: mysql-bin.002142
java -jar binlog-parser-tool.jar -hlocalhost -P3307 -utest -a mysql-bin.002142

# parser binlog file: mysql-bin.002142 from server->host:localhost,port:3307,user:test,password:123123 start at binlog file: mysql-bin.002142
java -jar binlog-parser-tool.jar -hlocalhost -P3307 -utest -p123123 mysql-bin.002142

# parser binlog file: D:\mysql-bin.002142 take String with GBK charset, and filter table name with 't1'
java -jar binlog-parser-tool.jar -c GBK -t t1 D:\mysql-bin.002142

# parser binlog file: D:\mysql-bin.002142 filter table name with 'sbtest' and the column of index 1 value as 12001(perhaps: id=12001) and start datetime: 2024-09-06 10:00:00
java -jar binlog-parser-tool.jar -w @1=12001 -t sbtest -m "2024-09-06 10:00:00" D:\mysql-bin.002142
```
