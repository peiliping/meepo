package meepo.transform.sink;

import meepo.transform.sink.log.SlowLogSink;
import meepo.transform.sink.parquet.ParquetSink;
import meepo.transform.sink.rdb.DBIgnoreSink;
import meepo.transform.sink.rdb.DBInsertSinkV2;
import meepo.transform.sink.rdb.DBReplaceSink;
import meepo.transform.sink.redis.RedisSinkV2;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SinkType {

    SLOWLOGSINK(SlowLogSink.class),

    DBINSERTSINK(DBInsertSinkV2.class),

    DBIGNORESINK(DBIgnoreSink.class),

    DBREPLACESINK(DBReplaceSink.class),

    REDISSINK(RedisSinkV2.class),

    PARQUETSINK(ParquetSink.class);

    public Class<? extends AbstractSink> clazz;

    SinkType(Class<? extends AbstractSink> clazz) {
        this.clazz = clazz;
    }
}
