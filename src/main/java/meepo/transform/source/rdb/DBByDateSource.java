package meepo.transform.source.rdb;

import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.util.dao.BasicDao;
import meepo.util.dao.ICallable;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * Created by peiliping on 17-3-11.
 */
public class DBByDateSource extends AbstractDBSource {

    public DBByDateSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        Validate.notBlank(context.get("primaryKeyName"));
        Pair<Long, Long> ps = BasicDao.autoGetStartEndDatePoint(super.dataSource, super.tableName, super.primaryKeyName, null);
        super.stepSize = context.getInteger("stepSize", 60000);
        super.start = context.getLong("start", ps.getLeft());
        super.end = context.getLong("end", ps.getRight());
        //long vStart = super.start - (super.start % super.stepSize);
        //super.currentPos = Math.max(vStart + index * super.stepSize, super.start);
        super.currentPos = super.start + index * super.stepSize;
        super.handler = new ICallable<Boolean>() {

            Timestamp vStart = new Timestamp(System.currentTimeMillis());
            Timestamp vEnd = new Timestamp(System.currentTimeMillis());

            @Override public void handleParams(PreparedStatement p) throws Exception {
                vStart.setTime(currentPos);
                vEnd.setTime(tmpEnd);
                p.setTimestamp(1, vStart);
                p.setTimestamp(2, vEnd);
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
