package meepo.transform.sink;

import com.google.common.collect.Lists;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.WorkHandler;
import lombok.Getter;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Created by peiliping on 17-3-3.
 */
public abstract class AbstractSink implements EventHandler, WorkHandler, TimeoutHandler, LifecycleAware {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    protected String taskName;

    protected int indexOfSinks;

    protected boolean RUNNING = false;

    @Getter protected List<Pair<String, Integer>> schema = Lists.newArrayList();

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

    @Override public void onStart() {
        LOG.info(this.taskName + "-Sink-" + this.indexOfSinks + "[" + this.getClass().getSimpleName() + "]" + " starting at " + LocalDateTime.now());
    }

    @Override public void onShutdown() {
        LOG.info(this.taskName + "-Sink-" + this.indexOfSinks + "[" + this.getClass().getSimpleName() + "]" + " ending at " + LocalDateTime.now());
    }

    public boolean isRunning() {
        return this.RUNNING;
    }
}
