package meepo.transform.source.rdb;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.util.Util;
import meepo.util.dao.ICallable;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;

/**
 * Created by peiliping on 17-7-14.
 */
public abstract class SparseDBSource extends AbstractDBSource {

    protected Iterator<Long> sparse;

    protected List<Long> params = Lists.newArrayList();

    public SparseDBSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        super.stepSize = context.getInteger("stepSize", 100);
        super.handler = new ICallable<Boolean>() {
            @Override
            public void handleParams(PreparedStatement p) throws Exception {
                for (int i = 0; i < params.size(); i++) {
                    p.setLong(i + 1, params.get(i));
                }
            }

            @Override
            public Boolean handleResultSet(ResultSet r) throws Exception {
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

    @Override
    public void work() {
        if (this.sparse == null || !this.sparse.hasNext()) {
            super.RUNNING = false;
            return;
        }
        if (this.params.isEmpty()) {
            while (this.params.size() < super.stepSize && this.sparse.hasNext()) {
                this.params.add(this.sparse.next());
            }
        }

        if (executeQuery()) {
            this.params.clear();
        } else {
            Util.sleep(1);
        }
    }

    @Override
    protected boolean executeQuery() {
        if (this.params.size() != super.stepSize) {
            super.sql = buildSQL(this.params.size());
        }
        return super.executeQuery();
    }

    @Override
    protected String buildSQL() {
        return buildSQL(0);
    }

    private String buildSQL(int size) {
        if (stepSize == 0)
            return null;
        else {
            List<String> ps = Lists.newArrayList();
            for (int i = 0; i < stepSize; i++)
                ps.add("?");
            return "SELECT " + super.columnNames + " FROM " + super.tableName + " WHERE " + this.primaryKeyName + " in (" + StringUtils.join(ps, ",") + ")";
        }
    }
}
