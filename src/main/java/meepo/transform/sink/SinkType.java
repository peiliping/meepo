package meepo.transform.sink;

import meepo.transform.sink.log.SlowLogSink;
import meepo.transform.sink.parquet.ParquetSink;
import meepo.transform.sink.rdb.DBIgnoreSink;
import meepo.transform.sink.rdb.DBInsertSink;
import meepo.transform.sink.rdb.DBInsertSinkV2;
import meepo.transform.sink.rdb.DBReplaceSink;
import meepo.transform.sink.redis.RedisSink;
import meepo.transform.sink.redis.RedisSinkV2;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SinkType {

    SLOWLOGSINK(SlowLogSink.class),

    DBINSERTSINK(DBInsertSink.class),

    DBINSERTSINKV2(DBInsertSinkV2.class),

    DBIGNORESINK(DBIgnoreSink.class),

    DBREPLACESINK(DBReplaceSink.class),

    REDISSINK(RedisSink.class),

    REDISSINKV2(RedisSinkV2.class),

    PARQUETSINK(ParquetSink.class);

    public Class<? extends AbstractSink> clazz;

    SinkType(Class<? extends AbstractSink> clazz) {
        this.clazz = clazz;
    }
}
