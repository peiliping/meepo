package meepo.transform.source;

import com.google.common.collect.Lists;
import lombok.Getter;
import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Created by peiliping on 17-3-2.
 */
public abstract class AbstractSource implements ISource {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractSource.class);

    protected String taskName;

    protected boolean RUNNING = false;

    protected RingbufferChannel channel;

    protected int indexOfSources;

    protected int totalSourceNum;

    protected int columnsNum;

    protected long tmpIndex;

    @Getter protected List<Pair<String, Integer>> schema = Lists.newArrayList();

    public AbstractSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        this.taskName = name;
        this.indexOfSources = index;
        this.totalSourceNum = totalNum;
        this.channel = rb;
    }

    @Override public void start() {
        this.RUNNING = true;
        LOG.info(this.taskName + "-Source-" + this.indexOfSources + "[" + this.getClass().getSimpleName() + "]" + " start at " + new Date());
    }

    @Override public void stop() {
        this.RUNNING = false;
    }

    @Override public void end() {
        this.RUNNING = false;
        LOG.info(this.taskName + "-Source-" + this.indexOfSources + "[" + this.getClass().getSimpleName() + "]" + " end at " + new Date());
    }

    private DataEvent de;

    @Override public DataEvent feedOne() {
        this.tmpIndex = this.channel.getNextSeq();
        this.de = this.channel.getBySeq(this.tmpIndex);
        if (!this.de.isInit()) {
            this.de.setSource(new Object[this.columnsNum]);
            this.de.setInit(true);
        }
        return this.de;
    }

    @Override public void pushOne() {
        this.channel.pushBySeq(this.tmpIndex);
    }

    @Override public void run() {
        start();
        while (this.RUNNING) {
            work();
        }
        end();
    }

    @Override public boolean isRunning() {
        return this.RUNNING;
    }
}
