package meepo.transform.sink;

import meepo.transform.sink.log.SlowLogSink;
import meepo.transform.sink.rdb.DBSink;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SinkType {

    SLOWLOGSINK(SlowLogSink.class),

    DBSINK(DBSink.class);


    public Class<? extends AbstractSink> clazz;

    SinkType(Class<? extends AbstractSink> clazz) {
        this.clazz = clazz;
    }
}
