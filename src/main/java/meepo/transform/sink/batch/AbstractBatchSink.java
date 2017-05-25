package meepo.transform.sink.batch;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;

/**
 * Created by peiliping on 17-5-24.
 */
public abstract class AbstractBatchSink extends AbstractSink {

    protected int stepSize;

    protected long lastCommit;

    protected long lastFlushTS;

    protected IHandler handler;

    public AbstractBatchSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.lastFlushTS = System.currentTimeMillis();
        this.stepSize = context.getInteger("stepSize", 100);
    }

    @Override public void onStart() {
        super.onStart();
        this.handler.init();
    }

    @Override public void onEvent(Object event) throws Exception {
        super.RUNNING = true;
        super.count++;
        this.handler.prepare(this.stepSize);
        DataEvent de = (DataEvent) event;
        this.handler.feed(de);
        if (super.count - this.lastCommit >= this.stepSize || System.currentTimeMillis() - this.lastFlushTS > 3000) {
            sinkFlush();
        }
    }

    protected void sinkFlush() {
        if (super.count - this.lastCommit > 0) {
            super.metricBatchCount++;
            if (super.count - this.lastCommit < this.stepSize) {
                super.metricUnsaturatedBatch++;
            }
            this.handler.flush();
        } else {
            this.handler.close();
        }
        this.lastCommit = super.count;
        super.RUNNING = false;
        this.lastFlushTS = System.currentTimeMillis();
    }

    @Override public void timeOut() {
        sinkFlush();
    }

    @Override public void onShutdown() {
        sinkFlush();
        super.onShutdown();
    }
}
