package meepo.transform.sink;

import meepo.transform.sink.log.SlowLogSink;

/**
 * Created by peiliping on 17-3-6.
 */
public enum SinkType {

    SLOWLOGSINK(SlowLogSink.class);


    public Class<? extends AbstractSink> clazz;

    SinkType(Class<? extends AbstractSink> clazz) {
        this.clazz = clazz;
    }
}
