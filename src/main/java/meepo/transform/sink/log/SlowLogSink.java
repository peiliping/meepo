package meepo.transform.sink.log;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.Util;

/**
 * Created by peiliping on 17-3-3.
 */
public class SlowLogSink extends AbstractSink {

    private int sleep;

    public SlowLogSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.sleep = context.getInteger("sleep", 1000);
    }

    @Override
    public void event(DataEvent event) {
        super.count++;
        if (this.sleep > 0)
            Util.sleepMS(this.sleep);
        LOG.info("event : " + event);
    }

    @Override
    public void timeOut() {
        LOG.info("time out.");
    }

}
