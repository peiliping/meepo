package meepo.transform.sink;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.rdb.handlers.IHandler;

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
        this.handler.feed(de, super.schema);
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
            super.RUNNING = false;
            this.lastCommit = super.count;
        } else {
            this.handler.close();
        }
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
