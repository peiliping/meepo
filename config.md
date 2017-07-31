Meepo 配置说明
===============

#### 声明任务列表(空格间隔)

meepo = test mysql2mysql mysql2parquet parquet2mysql parquet2vertica

#### Test 例子
```
meepo.test.source.type = SIMPLENUMSOURCE
meepo.test.channel.plugin.type = DEFAULT
meepo.test.sink.type = SLOWLOGSINK
```

#### Mysql 2 Mysql #####
```
meepo.mysql2mysql.source.type = DBBYIDSOURCE
meepo.mysql2mysql.source.tableName = app_entity
meepo.mysql2mysql.source.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysql.source.datasource.username = root
meepo.mysql2mysql.source.datasource.password = root
meepo.mysql2mysql.channel.plugin.type = DEFAULT
meepo.mysql2mysql.sink.type = DBINSERTSINK
meepo.mysql2mysql.sink.tableName = app_entity2
meepo.mysql2mysql.sink.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysql.sink.datasource.username = root
meepo.mysql2mysql.sink.datasource.password = root
```

##### Mysql 2 Parquet #####
```
meepo.mysql2parquet.source.type = DBBYIDSOURCE
meepo.mysql2parquet.source.tableName = app_entity
meepo.mysql2parquet.source.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2parquet.source.datasource.username = root
meepo.mysql2parquet.source.datasource.password = root
meepo.mysql2parquet.channel.plugin.type = PARQUETTYPECONVERT
meepo.mysql2parquet.sink.type = PARQUETSINK
meepo.mysql2parquet.sink.tableName = app_entity
meepo.mysql2parquet.sink.outputdir = /home/peiliping/dev/logs/
```


#### Parquet 2 Mysql #####
```
meepo.parquet2mysql.source.type = PARQUETFILESOURCE
meepo.parquet2mysql.source.inputdir = /home/peiliping/dev/logs/
meepo.parquet2mysql.channel.plugin.type = TYPECONVERT
meepo.parquet2mysql.sink.type = DBINSERTSINK
meepo.parquet2mysql.sink.tableName = app_entity3
meepo.parquet2mysql.sink.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.parquet2mysql.sink.datasource.username = root
meepo.parquet2mysql.sink.datasource.password = root
```

##### Parquet 2 Vertica #####
```
meepo.parquet2vertica.source.type = PARQUETFILESOURCE
meepo.parquet2vertica.source.inputdir = /home/peiliping/dev/logs/
meepo.parquet2vertica.channel.plugin.type = TYPECONVERT
meepo.parquet2vertica.sink.type = DBINSERTSINK
meepo.parquet2vertica.sink.tableName = app_entity3
meepo.parquet2vertica.sink.escapeColumnNames = false
meepo.parquet2vertica.sink.datasource.url = jdbc:vertica://127.0.0.1:5433/vertica?ssl=false
meepo.parquet2vertica.sink.datasource.driverClassName = com.vertica.jdbc.Driver
meepo.parquet2vertica.sink.datasource.validationQuery = select 1
meepo.parquet2vertica.sink.datasource.username = vertica
meepo.parquet2vertica.sink.datasource.password = vertica
```

一个较为完整的mysql jdbc url参数
```
rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8&amp;useSSL=false&amp;verifyServerCertificate=false&amp;failOverReadOnly=false&amp;autoReconnect=true&amp;autoReconnectForPools=true
```
