package meepo.transform.source.rdb;

import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.util.dao.BasicDao;
import meepo.util.dao.ICallable;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by peiliping on 17-3-7.
 */
public class DBByIdSource extends AbstractDBSource {

    public DBByIdSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        Pair<Long, Long> ps = BasicDao.autoGetStartEndPoint(super.dataSource, super.tableName, super.primaryKeyName, null);
        super.stepSize = context.getInteger("stepSize", 100);
        super.start = context.getLong("start", ps.getLeft());
        super.end = context.getLong("end", ps.getRight());
        long vStart = super.start - (super.start % super.stepSize);
        super.currentPos = Math.max(vStart + index * super.stepSize, super.start);
        super.handler = new ICallable<Boolean>() {
            @Override public void handleParams(PreparedStatement p) throws Exception {
                p.setLong(1, currentPos);
                p.setLong(2, tmpEnd);
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
