Meepo 配置说明
===============

##### 声明任务列表
meepo = test mysql2mysql mysql2mysqlbydate parquet2mysql mysql2parquet mysql2mysqlreplace

##### Test 例子
```
meepo.test.source.type = SIMPLENUMSOURCE
meepo.test.channel.bufferSize = 16
meepo.test.sink.type = SLOWLOGSINK
#meepo.test.channel.delay = 3
#meepo.test.channel.plugin.type = DEFAULT
#meepo.test.sink.sleep = 1000
```

##### Mysql 2 Mysql #####
```
meepo.mysql2mysql.source.type = DBBYIDSOURCE
meepo.mysql2mysql.source.tableName = app_entity
#meepo.mysql2mysql.source.workersNum = 1
#meepo.mysql2mysql.source.stepSize = 100
#meepo.mysql2mysql.source.primaryKeyName = [AUTO]
#meepo.mysql2mysql.source.columnNames = [AUTO]
#meepo.mysql2mysql.source.extraSQL = 
#meepo.mysql2mysql.source.start = [AUTO]
#meepo.mysql2mysql.source.end = [AUTO]
meepo.mysql2mysql.source.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysql.source.datasource.username = root
meepo.mysql2mysql.source.datasource.password = root

meepo.mysql2mysql.channel.bufferSize = 16
meepo.mysql2mysql.channel.plugin.type = TYPECONVERT

meepo.mysql2mysql.sink.type = DBINSERTSINK
meepo.mysql2mysql.sink.tableName = app_entity2
#meepo.mysql2mysql.sink.workersNum = 1
#meepo.mysql2mysql.sink.stepSize = 100
#meepo.mysql2mysql.sink.primaryKeyName = [AUTO]
#meepo.mysql2mysql.sink.columnNames = [AUTO]
meepo.mysql2mysql.sink.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysql.sink.datasource.username = root
meepo.mysql2mysql.sink.datasource.password = root
```

##### Mysql 2 Mysql By Date #####
```
meepo.mysql2mysqlbydate.source.type = DBBYDATESOURCE
meepo.mysql2mysqlbydate.source.tableName = app_entity
meepo.mysql2mysqlbydate.source.primaryKeyName = create_time
meepo.mysql2mysqlbydate.source.stepSize = 86400000
#meepo.mysql2mysqlbydate.source.workersNum = 1
#meepo.mysql2mysqlbydate.source.columnNames = [AUTO]
#meepo.mysql2mysqlbydate.source.extraSQL = 
#meepo.mysql2mysqlbydate.source.start = [AUTO]
#meepo.mysql2mysqlbydate.source.end = [AUTO]
meepo.mysql2mysqlbydate.source.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlbydate.source.datasource.username = root
meepo.mysql2mysqlbydate.source.datasource.password = root

meepo.mysql2mysqlbydate.channel.bufferSize = 16
meepo.mysql2mysqlbydate.channel.plugin.type = TYPECONVERT

meepo.mysql2mysqlbydate.sink.type = DBINSERTSINK
meepo.mysql2mysqlbydate.sink.tableName = app_entity3
#meepo.mysql2mysqlbydate.sink.workersNum = 1
#meepo.mysql2mysqlbydate.sink.stepSize = 100
#meepo.mysql2mysqlbydate.sink.primaryKeyName = [AUTO]
#meepo.mysql2mysqlbydate.sink.columnNames = [AUTO]
meepo.mysql2mysqlbydate.sink.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlbydate.sink.datasource.username = root
meepo.mysql2mysqlbydate.sink.datasource.password = root
```

##### Parquet 2 Mysql #####
```
meepo.parquet2mysql.source.type = PARQUETFILESOURCE
meepo.parquet2mysql.source.inputdir = /home/peiliping/dev/logs/

meepo.parquet2mysql.channel.bufferSize = 16
meepo.parquet2mysql.channel.plugin.type = TYPECONVERT

meepo.parquet2mysql.sink.type = DBINSERTSINK
meepo.parquet2mysql.sink.tableName = app_entity3
meepo.parquet2mysql.sink.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.parquet2mysql.sink.datasource.username = root
meepo.parquet2mysql.sink.datasource.password = root
```

##### Mysql 2 Parquet #####
```
meepo.mysql2parquet.source.type = DBBYIDSOURCE
meepo.mysql2parquet.source.tableName = app_entity
meepo.mysql2parquet.source.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2parquet.source.datasource.username = root
meepo.mysql2parquet.source.datasource.password = root

meepo.mysql2parquet.channel.bufferSize = 16
meepo.mysql2parquet.channel.plugin.type = PARQUETTYPECONVERT

meepo.mysql2parquet.sink.type = PARQUETSINK
meepo.mysql2parquet.sink.tableName = app_entity
meepo.mysql2parquet.sink.outputdir = /home/peiliping/dev/logs/
#meepo.mysql2parquet.sink.rollingsize = 
#meepo.mysql2parquet.sink.hdfsconfdir = 
```

##### Mysql 2 Mysql Replace #####
```
meepo.mysql2mysqlreplace.source.type = DBBYIDSOURCE
meepo.mysql2mysqlreplace.tableName = app_entity
meepo.mysql2mysqlreplace.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlreplace.datasource.username = root
meepo.mysql2mysqlreplace.datasource.password = root

meepo.mysql2mysqlreplace.channel.bufferSize = 16
meepo.mysql2mysqlreplace.channel.plugin.type = REPLACEPLUGIN
meepo.mysql2mysqlreplace.channel.plugin.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlreplace.channel.plugin.datasource.username = root
meepo.mysql2mysqlreplace.channel.plugin.datasource.password = root
meepo.mysql2mysqlreplace.channel.plugin.tableName = vsid
meepo.mysql2mysqlreplace.channel.plugin.replacePosition = 3
meepo.mysql2mysqlreplace.channel.plugin.replaceFieldName = metric_id
meepo.mysql2mysqlreplace.channel.plugin.keyName = id
meepo.mysql2mysqlreplace.channel.plugin.valName = val
meepo.mysql2mysqlreplace.channel.plugin.cacheSize = 1000
meepo.mysql2mysqlreplace.channel.plugin.null4null = false

meepo.mysql2mysqlreplace.sink.type = DBINSERTSINK
meepo.mysql2mysqlreplace.sink.tableName = app_entity2
meepo.mysql2mysqlreplace.sink.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlreplace.sink.datasource.username = root
meepo.mysql2mysqlreplace.sink.datasource.password = root
```

##### Mysql 2 Mysql Group Replace #####
```
meepo.mysql2mysqlreplace.source.type = DBBYIDSOURCE
meepo.mysql2mysqlreplace.tableName = app_entity
meepo.mysql2mysqlreplace.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlreplace.datasource.username = root
meepo.mysql2mysqlreplace.datasource.password = root

meepo.mysql2mysqlreplace.channel.bufferSize = 16
meepo.mysql2mysqlreplace.channel.plugin.type = GROUPPLUGIN
meepo.mysql2mysqlreplace.channel.plugin.array = REPLACEPLUGIN-rpp1 REPLACEPLUGIN-rpp2 
meepo.mysql2mysqlreplace.channel.plugin.rpp1.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlreplace.channel.plugin.rpp1.datasource.username = root
meepo.mysql2mysqlreplace.channel.plugin.rpp1.datasource.password = root
meepo.mysql2mysqlreplace.channel.plugin.rpp1.tableName = vsid
meepo.mysql2mysqlreplace.channel.plugin.rpp1.replacePosition = 3
meepo.mysql2mysqlreplace.channel.plugin.rpp1.replaceFieldName = metric_id
meepo.mysql2mysqlreplace.channel.plugin.rpp1.keyName = id
meepo.mysql2mysqlreplace.channel.plugin.rpp1.valName = val
meepo.mysql2mysqlreplace.channel.plugin.rpp1.cacheSize = 1000
meepo.mysql2mysqlreplace.channel.plugin.rpp1.null4null = false

meepo.mysql2mysqlreplace.channel.plugin.rpp2.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlreplace.channel.plugin.rpp2.datasource.username = root
meepo.mysql2mysqlreplace.channel.plugin.rpp2.datasource.password = root
meepo.mysql2mysqlreplace.channel.plugin.rpp2.tableName = vsid
meepo.mysql2mysqlreplace.channel.plugin.rpp2.replacePosition = 4
meepo.mysql2mysqlreplace.channel.plugin.rpp2.replaceFieldName = type_id
meepo.mysql2mysqlreplace.channel.plugin.rpp2.keyName = id
meepo.mysql2mysqlreplace.channel.plugin.rpp2.valName = val
meepo.mysql2mysqlreplace.channel.plugin.rpp2.cacheSize = 1000
meepo.mysql2mysqlreplace.channel.plugin.rpp2.null4null = false

meepo.mysql2mysqlreplace.sink.type = DBINSERTSINK
meepo.mysql2mysqlreplace.sink.tableName = app_entity2
meepo.mysql2mysqlreplace.sink.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2mysqlreplace.sink.datasource.username = root
meepo.mysql2mysqlreplace.sink.datasource.password = root
```

##### Mysql 2 Redis #####
```
meepo.mysql2redis.source.type = DBBYIDSOURCE
meepo.mysql2redis.source.tableName = app_entity
meepo.mysql2redis.source.datasource.url = jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8
meepo.mysql2redis.source.datasource.username = root
meepo.mysql2redis.source.datasource.password = root

meepo.mysql2redis.channel.bufferSize = 16
meepo.mysql2redis.channel.plugin.type = TYPECONVERT

meepo.mysql2redis.sink.type = REDISSINK
meepo.mysql2redis.sink.address = 127.0.0.1:6379
```

##### Parquet 2 ElasticSearch #####
```
meepo.parquet2es.source.type = PARQUETFILESOURCE
meepo.parquet2es.source.workersNum = 1
meepo.parquet2es.source.inputdir = /home/peiliping/dev/logs/

meepo.parquet2es.channel.bufferSize = 16
meepo.parquet2es.channel.plugin.type = TYPECONVERT

meepo.parquet2es.sink.type = ELASTICSINK
meepo.parquet2es.sink.workersNum = 1
meepo.parquet2es.sink.stepsize = 1000
meepo.parquet2es.sink.address = 127.0.0.1
meepo.parquet2es.sink.port = 9300
```

一个较为完整的jdbc url参数
```
rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8&amp;useSSL=false&amp;verifyServerCertificate=false&amp;failOverReadOnly=false&amp;autoReconnect=true&amp;autoReconnectForPools=true
```