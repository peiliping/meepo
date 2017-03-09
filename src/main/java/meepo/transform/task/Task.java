package meepo.transform.task;

import com.google.common.collect.Lists;
import com.lmax.disruptor.EventProcessor;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.transform.sink.SinkType;
import meepo.transform.source.AbstractSource;
import meepo.transform.source.SourceType;
import meepo.util.Constants;
import meepo.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by peiliping on 17-3-3.
 */
public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    private String taskName;

    private long createTime;

    private long finishedTime;

    private AtomicBoolean RUNNING = new AtomicBoolean(false);

    private RingbufferChannel channel;

    private ThreadPoolExecutor sourcesPool;

    private Class<? extends AbstractSource> sourceClazz;

    private TaskContext sourceContext;

    private int sourceNum;

    private List<AbstractSource> sources = Lists.newArrayList();

    private ThreadPoolExecutor sinksPool;

    private Class<? extends AbstractSink> sinkClazz;

    private TaskContext sinkContext;

    private int sinkNum;

    private List<EventProcessor> sinks = Lists.newArrayList();

    public Task(String name) {
        this.taskName = name;
        this.createTime = System.currentTimeMillis();
    }

    public void init(TaskContext taskContext) {
        this.sourceContext = new TaskContext(this.taskName + "-" + Constants.SOURCE, taskContext.getSubProperties(Constants.SOURCE_));
        this.sourceClazz = (SourceType.valueOf(this.sourceContext.getString("type"))).clazz;
        this.sourceNum = sourceContext.getInteger("workersNum", 1);
        this.sourcesPool = new ThreadPoolExecutor(this.sourceNum, this.sourceNum, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        TaskContext channelContext = new TaskContext(this.taskName + "-" + Constants.CHANNEL, taskContext.getSubProperties(Constants.CHANNEL_));
        this.channel = new RingbufferChannel(this.sourceNum, channelContext);

        this.sinkContext = new TaskContext(this.taskName + "-" + Constants.SINK, taskContext.getSubProperties(Constants.SINK_));
        this.sinkClazz = (SinkType.valueOf(this.sinkContext.getString("type", SinkType.SLOWLOGSINK.name()))).clazz;
        this.sinkNum = this.sinkContext.getInteger("workersNum", 1);
        this.sinksPool = new ThreadPoolExecutor(this.sinkNum, this.sinkNum, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    public void start() throws Exception {
        LOG.info("Task[" + this.taskName + "]" + " is starting ...");
        AbstractSink[] sinkHandlers = new AbstractSink[this.sinkNum];
        for (int i = 0; i < this.sinkNum; i++) {
            sinkHandlers[i] = this.sinkClazz.getConstructor(String.class, int.class, TaskContext.class).newInstance(this.taskName, i, this.sinkContext);
        }
        this.sinks.addAll(this.channel.start(sinkHandlers));
        for (int i = 0; i < this.sourceNum; i++) {
            this.sources.add(this.sourceClazz.getConstructor(String.class, int.class, int.class, TaskContext.class, RingbufferChannel.class)
                    .newInstance(this.taskName, i, this.sourceNum, this.sourceContext, this.channel));
        }
        this.channel.autoMatchSchema(this.sources.get(0).getSchema(), sinkHandlers[0].getSchema());
        this.sinks.forEach(ep -> this.sinksPool.submit(ep));
        this.sources.forEach(as -> this.sourcesPool.submit(as));
        this.RUNNING.set(true);
        LOG.info("Task[" + this.taskName + "]" + " started ...");
    }

    public void close() {
        LOG.info("Task[" + this.taskName + "]" + " is closing ...");
        this.RUNNING.set(false);
        this.sources.forEach(as -> as.stop());
        if (!this.sourcesPool.isShutdown()) {
            this.sourcesPool.shutdownNow();
        }
        while (!this.channel.isEmpty()) {
            Util.sleep(1);
        }
        Util.sleep(5);
        this.sinks.forEach(ep -> ep.halt());
        if (!this.sinksPool.isShutdown()) {
            this.sinksPool.shutdownNow();
        }
        if (this.finishedTime == 0) {
            this.finishedTime = System.currentTimeMillis();
        }
        LOG.info("Task[" + this.taskName + "]" + " closed ..." + LocalDateTime.now());
    }

    public boolean recycle() {
        for (AbstractSource as : this.sources) {
            if (as.isRunning()) {
                return false;
            }
        }
        close();
        return true;
    }
}
