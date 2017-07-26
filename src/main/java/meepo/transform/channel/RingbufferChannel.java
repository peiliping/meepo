package meepo.transform.channel;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Getter;
import meepo.transform.channel.plugin.AbstractPlugin;
import meepo.transform.channel.plugin.PluginType;
import meepo.transform.config.TaskContext;
import meepo.transform.report.ChannelReportItem;
import meepo.transform.report.IReportItem;
import meepo.transform.sink.AbstractSink;
import meepo.util.Constants;
import meepo.util.Util;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by peiliping on 17-3-2.
 */
public class RingbufferChannel {

    private static final Logger LOG = LoggerFactory.getLogger(RingbufferChannel.class);

    private final RingBuffer<DataEvent> ringBuffer;

    private final SequenceBarrier seqBarrier;

    private final AtomicBoolean STARTED = new AtomicBoolean(false);

    @Getter
    private AbstractPlugin plugin;

    public RingbufferChannel(int sourcesCount, TaskContext context) {
        int bufferSize = context.getInteger("bufferSize", 1024);
        int tolerableDelaySeconds = context.getInteger("delay", 3);
        ProducerType producerType = sourcesCount > 1 ? ProducerType.MULTI : ProducerType.SINGLE;
        this.ringBuffer = RingBuffer.create(producerType, DataEvent.INT_ENEVT_FACTORY, bufferSize,
                new TimeoutBlockingWaitStrategy(tolerableDelaySeconds, TimeUnit.SECONDS));//LiteTimeoutBlockingWaitStrategy(tolerableDelaySeconds, TimeUnit.SECONDS)
        this.seqBarrier = this.ringBuffer.newBarrier();
        TaskContext pluginContext = new TaskContext(context.getTaskName() + "-" + Constants.PLUGIN, context.getSubProperties(Constants.PLUGIN_));
        Class<? extends AbstractPlugin> pluginClazz = (PluginType.valueOf(pluginContext.getString("type", PluginType.DEFAULT.name()))).clazz;
        try {
            this.plugin = pluginClazz.getConstructor(TaskContext.class).newInstance(pluginContext);
        } catch (Exception e) {
            LOG.error("RingBuffer Plugin Init Error : ", e);
        }
    }

    public List<EventProcessor> integrateSinks(AbstractSink... handlers) {
        checkRunning();
        List<EventProcessor> wps = Lists.newArrayList();
        if (handlers.length == 1) {
            BatchEventProcessor<DataEvent> processor = new BatchEventProcessor<DataEvent>(this.ringBuffer, this.seqBarrier, handlers[0]);
            this.ringBuffer.addGatingSequences(processor.getSequence());
            wps.add(processor);
        } else {
            Sequence consumerSequence = new Sequence(-1);
            for (AbstractSink wh : handlers) {
                WorkProcessor<DataEvent> processor = new WorkProcessor<DataEvent>(this.ringBuffer, this.seqBarrier, wh, new IgnoreExceptionHandler(), consumerSequence);
                this.ringBuffer.addGatingSequences(processor.getSequence());
                wps.add(processor);
            }
        }
        return wps;
    }

    public void prepare(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        this.plugin.autoMatchSchema(source, sink);
        LOG.info("Source Schema : " + JSON.toJSONString(source, true));
        LOG.info("Sink Schema : " + JSON.toJSONString(sink, true));
    }

    private void checkRunning() {
        Validate.isTrue(!this.STARTED.get());
        this.STARTED.set(true);
    }

    public long getNextSeq() {
        return this.ringBuffer.next();
    }

    public DataEvent getBySeq(long seq) {
        return this.ringBuffer.get(seq);
    }

    public void pushBySeq(long seq) {
        try {
            this.plugin.convert(this.ringBuffer.get(seq), true);
            this.ringBuffer.publish(seq);
        } catch (Throwable e) {
            LOG.error("Convert Function Error :", e);
            Util.sleep(1);
            pushBySeq(seq);
        }
    }

    public boolean isCovered(long seq) {
        return seq > this.ringBuffer.getBufferSize();
    }

    public boolean isEmpty() {
        return this.ringBuffer.remainingCapacity() == this.ringBuffer.getBufferSize();
    }

    public IReportItem report() {
        return ChannelReportItem.builder().name("channel").capacity(this.ringBuffer.getBufferSize()).remain(this.ringBuffer.remainingCapacity()).build();
    }

    public void close() {
        this.plugin.close();
    }

}
