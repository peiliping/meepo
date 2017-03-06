package meepo.transform.sink.log;

import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.Util;

/**
 * Created by peiliping on 17-3-3.
 */
public class SlowLogSink extends AbstractSink {

    public SlowLogSink(String name, int index, TaskContext context) {
        super(name, index, context);
    }

    @Override public void onEvent(Object event) throws Exception {
        Util.sleep(1);
        super.LOG.info("event : " + event);
    }

    @Override public void timeOut() {
        super.LOG.info("time out.");
    }
}
