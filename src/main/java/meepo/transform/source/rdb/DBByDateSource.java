package meepo.transform.source.rdb;

import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.util.dao.BasicDao;
import meepo.util.dao.ICallable;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by peiliping on 17-3-11.
 */
public class DBByDateSource extends AbstractDBSource {

    public DBByDateSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        Validate.notBlank(context.get("primaryKeyName"));
        Pair<Long, Long> ps = BasicDao.autoGetStartEndDatePoint(this.dataSource, this.tableName, this.primaryKeyName);
        this.stepSize = context.getInteger("stepSize", 60000);
        this.start = context.getLong("start", ps.getLeft());
        this.end = context.getLong("end", ps.getRight());
        long vStart = this.start - (this.start % this.stepSize);
        this.currentPos = Math.max(vStart + index * this.stepSize, this.start);
        this.handler = new ICallable<Boolean>() {
            @Override public void handleParams(PreparedStatement p) throws Exception {
                p.setDate(1, new Date(currentPos));
                p.setDate(2, new Date(tmpEnd));
            }

            @Override public Boolean handleResultSet(ResultSet r) throws Exception {
                while (r.next() && RUNNING) {
                    DataEvent de = feedOne();
                    for (int i = 1; i <= columnsNum; i++) {
                        de.getSource()[i - 1] = r.getObject(i);
                    }
                    pushOne();
                }
                return true;
            }
        };
    }
}
