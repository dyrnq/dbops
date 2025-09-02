# dbops

Inspired by [dbhub](https://github.com/yeqown/dbhub).

## config

```bash
cat > ~/.dbops/config.yaml<<EOF
solon:
  dataSources:
    default!:
      class: com.zaxxer.hikari.HikariDataSource
      jdbcUrl: jdbc:mysql://192.168.76.30:13306/example?characterEncoding=utf-8&useSSL=false&autoReconnect=true&allowPublicKeyRetrieval=true
      driverClassName: com.mysql.cj.jdbc.Driver
      username: root
      password: 123456
EOF
```
## usage showcase

### exec

```bash
java -jar target/dbops.jar exec --sql "SHOW BINARY LOGS" | jq
java -jar target/dbops.jar exec --sql "SHOW BINARY LOGS" | jq -r '.[].Log_name'
java -jar target/dbops.jar exec --sql "SHOW VARIABLES LIKE 'log_%';" | jq
java -jar target/dbops.jar exec --sql "SHOW VARIABLES LIKE '%innodb_log%';" | jq
java -jar target/dbops.jar exec --sql "SHOW MASTER STATUS;" | jq
java -jar target/dbops.jar exec --sql "SHOW GLOBAL STATUS LIKE '%Connections%';" | jq
java -jar target/dbops.jar exec --sql "SHOW GLOBAL STATUS LIKE '%Queries%';" | jq
java -jar target/dbops.jar exec --sql "SHOW GLOBAL STATUS LIKE '%Uptime%';" | jq
java -jar target/dbops.jar exec --sql "SHOW GLOBAL STATUS;" | jq
java -jar target/dbops.jar exec -A --sql "SELECT * FROM information_schema.SCHEMATA;" | jq -r '.[].SCHEMA_NAME'
java -jar target/dbops.jar exec -A --sql "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA = 'example';" | jq -r ".[].TABLE_NAME"
java -jar target/dbops.jar exec --sql "SHOW VARIABLES LIKE '%password%';" | jq
java -jar target/dbops.jar exec --sql "SHOW SLAVES;" | jq
java -jar target/dbops.jar exec --sql "SHOW VARIABLES LIKE '%gtid%';" | jq
java -jar target/dbops.jar exec --sql "SHOW CREATE TABLE example.hello;" | jq -r '."Create Table"'
```
### schema-diff

```bash
java -jar target/dbops.jar schema-diff --source-schema example --target-schema test --type table
```

### schema-to-doris

```bash
java -jar target/dbops.jar schema-to-doris --source-schema example --target-schema testdb --type olap
```