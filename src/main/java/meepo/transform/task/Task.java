package meepo.transform.task;

import com.google.common.collect.Lists;
import com.lmax.disruptor.EventProcessor;
import jdk.nashorn.internal.runtime.Source;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.transform.sink.SinkType;
import meepo.transform.source.AbstractSource;
import meepo.transform.source.SourceType;
import meepo.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by peiliping on 17-3-3.
 */
public class Task implements Closeable {

    protected static final Logger LOG = LoggerFactory.getLogger(Task.class);

    protected String taskName;

    protected long createTime;

    protected long finishedTime;

    protected RingbufferChannel channel;

    protected ThreadPoolExecutor sourcesPool;

    protected Class<? extends AbstractSource> sourceClazz;

    protected TaskContext sourceContext;

    protected int sourceNum;

    protected List<AbstractSource> sources = Lists.newArrayList();

    protected ThreadPoolExecutor sinksPool;

    protected Class<? extends AbstractSink> sinkClazz;

    protected TaskContext sinkContext;

    protected int sinkNum;

    protected List<EventProcessor> sinks = Lists.newArrayList();

    public Task(String name) {
        this.taskName = name;
        this.createTime = System.currentTimeMillis();
    }

    public void init(TaskContext taskContext) {
        {
            this.sourceContext = new TaskContext(this.taskName + "-Source", taskContext.getSubProperties("source."));
            this.sourceClazz = (SourceType.valueOf(this.sourceContext.getString("type", SourceType.SIMPLENUMSOURCE.name()))).clazz;
            this.sourceNum = sourceContext.getInteger("workersNum", 1);
            this.sourcesPool = new ThreadPoolExecutor(this.sourceNum, this.sourceNum, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        }
        {
            TaskContext channelContext = new TaskContext(this.taskName + "-Channel", taskContext.getSubProperties("channel."));
            int bufferSize = channelContext.getInteger("bufferSize", 1024);
            int tolerableDelaySeconds = channelContext.getInteger("delay", 3);
            this.channel = new RingbufferChannel(bufferSize, this.sourceNum, tolerableDelaySeconds);
        }
        {
            this.sinkContext = new TaskContext(this.taskName + "-Sink", taskContext.getSubProperties("sink."));
            this.sinkClazz = (SinkType.valueOf(this.sinkContext.getString("type", SinkType.SLOWLOGSINK.name()))).clazz;
            this.sinkNum = this.sinkContext.getInteger("workersNum", 1);
            this.sinksPool = new ThreadPoolExecutor(this.sinkNum, this.sinkNum, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        }
    }

    public void start() throws Exception {
        AbstractSink[] sinkHandlers = new AbstractSink[this.sinkNum];
        for (int i = 0; i < this.sinkNum; i++) {
            sinkHandlers[i] = this.sinkClazz.getConstructor(String.class, int.class, TaskContext.class).newInstance(this.taskName, i, this.sinkContext);
        }
        this.sinks.addAll(this.channel.start(sinkHandlers));
        this.sinks.forEach(ep -> this.sinksPool.submit(ep));

        for (int i = 0; i < this.sourceNum; i++) {
            this.sources.add(this.sourceClazz.getConstructor(String.class, int.class, TaskContext.class, RingbufferChannel.class)
                    .newInstance(this.taskName, i, this.sourceContext, this.channel));
        }
        this.sources.forEach(as -> this.sourcesPool.submit(as));
    }

    @Override public void close() throws IOException {
        LOG.info("Task[" + this.taskName + "]" + "is closing ...");
        this.sources.forEach(as -> as.end());
        this.sourcesPool.shutdownNow();
        while (!this.channel.isEmpty()) {
            Util.sleep(1);
        }
        this.sinks.forEach(ep -> ep.halt());
        this.sinksPool.shutdownNow();
        this.finishedTime = System.currentTimeMillis();
        LOG.info("Task[" + this.taskName + "]" + " closed ..." + new Date());
    }

    public boolean checkSourcesFinished() {
        for (AbstractSource as : this.sources) {
            if (as.isRunning()) {
                return false;
            }
        }
        try {
            close();
        } catch (IOException e) {
        }
        return true;
    }
}
