package meepo.transform.source.rdb;

import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import org.apache.commons.lang3.Validate;

/**
 * Created by peiliping on 17-3-10.
 */
public class DBSyncByTSSource extends DBSyncSource {

    private long delay;

    private long now;

    public DBSyncByTSSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        Validate.notBlank(context.get("primaryKeyName"));
        Validate.isTrue(totalNum == 1);
        this.delay = context.getLong("delay", 5000L);
        this.stepSize = context.getInteger("stepSize", 60000);
        if (context.get("start") == null) {
            this.now = System.currentTimeMillis();
            super.start = this.now - this.delay;
            super.currentPos = super.start;
        }
    }

    @Override public void work() {
        this.startEnd = BasicDao.autoGetStartEndPoint(super.dataSource, super.tableName, super.primaryKeyName);
        this.now = System.currentTimeMillis();
        this.tmpEnd = Math.min(this.currentPos + this.stepSize, Math.min(this.now - this.delay, this.startEnd.getRight()));
        if (this.tmpEnd == this.currentPos) {
            this.currentPos = this.now - this.delay;
            Util.sleep(1);
            return;
        }
        if (executeQuery()) {
            super.currentPos = super.tmpEnd;
        }
    }
}
