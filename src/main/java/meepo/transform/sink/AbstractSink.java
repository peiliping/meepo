package meepo.transform.sink;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.WorkHandler;
import meepo.transform.config.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peiliping on 17-3-3.
 */
public abstract class AbstractSink implements EventHandler, WorkHandler, TimeoutHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    protected String taskName;

    protected int indexOfSinks;

    public AbstractSink(String name, int index, TaskContext context) {
        this.taskName = name;
        this.indexOfSinks = index;
    }

    public abstract void timeOut();

    @Override public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        onEvent(event);
    }

    @Override public void onTimeout(long sequence) throws Exception {
        timeOut();
    }
}
