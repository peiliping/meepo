package meepo.transform.sink;

import meepo.transform.sink.redis.RedisSink;
import meepo.transform.sink.log.SlowLogSink;
import meepo.transform.sink.parquet.ParquetSink;
import meepo.transform.sink.rdb.DBIgnoreSink;
import meepo.transform.sink.rdb.DBInsertSink;
import meepo.transform.sink.rdb.DBReplaceSink;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SinkType {

    SLOWLOGSINK(SlowLogSink.class),

    DBINSERTSINK(DBInsertSink.class),

    DBIGNORESINK(DBIgnoreSink.class),

    DBREPLACESINK(DBReplaceSink.class),

    REDISSINK(RedisSink.class),

    PARQUETSINK(ParquetSink.class);

    public Class<? extends AbstractSink> clazz;

    SinkType(Class<? extends AbstractSink> clazz) {
        this.clazz = clazz;
    }
}
