package meepo.transform.source.rdb;

import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.util.dao.BasicDao;

/**
 * Created by peiliping on 17-3-10.
 */
public class DBSyncByTSSource extends DBSyncSource {

    private long delay;

    private long now;

    public DBSyncByTSSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        this.delay = context.getLong("delay", 5000L);
    }

    @Override public void work() {
        this.startEnd = BasicDao.autoGetStartEndPoint(super.dataSource, super.tableName, super.primaryKeyName);
        this.now = System.currentTimeMillis();
        this.tmpEnd = Math.min(this.currentPos + this.stepSize, Math.min(this.now - this.delay, this.startEnd.getRight()));
        if (executeQuery()) {
            super.currentPos = super.tmpEnd;
        }
    }
}
