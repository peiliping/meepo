package meepo.transform.source.fake;

import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.transform.source.AbstractSource;

/**
 * Created by peiliping on 17-3-6.
 */
public class SimpleNumSource extends AbstractSource {

    public SimpleNumSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
    }

    @Override public void eventFactory(DataEvent de) {
        de.setSource(new Long[2]);
    }

    @Override public void work() {
        DataEvent de = super.feedOne();
        de.getSource()[0] = Long.valueOf(super.indexOfSources);
        de.getSource()[1] = super.tmpIndex;
        super.pushOne();
    }
}
