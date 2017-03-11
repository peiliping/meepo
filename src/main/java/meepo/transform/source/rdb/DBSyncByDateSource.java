package meepo.transform.source.rdb;

import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by peiliping on 17-3-10.
 */
public class DBSyncByDateSource extends DBByDateSource {

    private Pair<Long, Long> startEnd;

    private long delay;

    private long now;

    public DBSyncByDateSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        Validate.notBlank(context.get("primaryKeyName"));
        Validate.isTrue(totalNum == 1);
        super.end = Long.MAX_VALUE;
        this.startEnd = BasicDao.autoGetStartEndDatePoint(this.dataSource, this.tableName, this.primaryKeyName);
        this.delay = context.getLong("delay", 5000L);
        this.now = System.currentTimeMillis();
        super.start = context.getLong("start", this.now - this.delay);
        super.currentPos = super.start;
    }

    @Override public void work() {
        this.startEnd = BasicDao.autoGetStartEndDatePoint(super.dataSource, super.tableName, super.primaryKeyName);
        this.now = System.currentTimeMillis();
        super.tmpEnd = Math.min(super.currentPos + super.stepSize, Math.min(this.now - this.delay, this.startEnd.getRight()));
        if (super.tmpEnd == super.currentPos) {
            super.currentPos = this.now - this.delay;
            Util.sleep(1);
            return;
        }
        if (executeQuery()) {
            super.currentPos = super.tmpEnd;
        }
    }
}
