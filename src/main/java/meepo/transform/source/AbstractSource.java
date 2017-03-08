package meepo.transform.source;

import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by peiliping on 17-3-2.
 */
public abstract class AbstractSource implements ISource {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractSource.class);

    protected String taskName;

    protected int indexOfSources;

    protected RingbufferChannel channel;

    protected boolean RUNNING = false;

    protected long tmpIndex;

    protected int totalSourceNum;

    public AbstractSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        this.taskName = name;
        this.indexOfSources = index;
        this.totalSourceNum = totalNum;
        this.channel = rb;
    }

    @Override public void start() {
        this.RUNNING = true;
        LOG.info(this.taskName + "-Source-" + this.indexOfSources + "[" + this.getClass().getSimpleName() + "]" + " starting at " + new Date());
    }

    @Override public void stop() {
        this.RUNNING = false;
    }

    @Override public void end() {
        this.RUNNING = false;
        LOG.info(this.taskName + "-Source-" + this.indexOfSources + "[" + this.getClass().getSimpleName() + "]" + " ending at " + new Date());
    }

    @Override public DataEvent feedOne() {
        this.tmpIndex = this.channel.getNextSeq();
        DataEvent de = this.channel.getBySeq(this.tmpIndex);
        if (!de.isInit()) {
            eventFactory(de);
            de.setInit(true);
        }
        return de;
    }

    @Override public void run() {
        start();
        while (RUNNING) {
            work();
        }
        end();
    }

    @Override public boolean isRunning() {
        return this.RUNNING;
    }
}
