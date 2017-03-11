package meepo.transform.source.rdb;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.transform.source.AbstractSource;
import meepo.util.Constants;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import meepo.util.dao.ICallable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Created by peiliping on 17-3-11.
 */
public class DBByDateSource extends AbstractSource {

    protected DataSource dataSource;

    protected String tableName;

    protected String primaryKeyName;

    protected int stepSize;

    protected String columnNames;

    protected String extraSQL;

    protected long start;

    protected long end;

    protected long currentPos;

    protected long tmpEnd;

    protected String sql;

    protected ICallable<Boolean> handler;

    public DBByDateSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.primaryKeyName = context.getString("primaryKeyName");
        this.columnNames = context.getString("columnNames", "*");
        this.extraSQL = context.getString("extraSQL", "");

        final List<String> columnsArray = Lists.newArrayList();
        super.schema = BasicDao.parserSchema(this.dataSource, this.tableName, this.columnNames, this.primaryKeyName);
        super.schema.forEach(item -> columnsArray.add(item.getLeft()));
        this.columnNames = StringUtils.join(columnsArray, ",");
        super.columnsNum = super.schema.size();

        Pair<Long, Long> ps = BasicDao.autoGetStartEndDatePoint(this.dataSource, this.tableName, this.primaryKeyName);
        this.stepSize = context.getInteger("stepSize", 60000);
        this.start = context.getLong("start", ps.getLeft());
        this.end = context.getLong("end", ps.getRight());
        long vStart = this.start - (this.start % this.stepSize);
        this.currentPos = Math.max(vStart + index * this.stepSize, this.start);

        this.sql = buildSQL();
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

    @Override public void work() {
        if (this.currentPos >= this.end) {
            super.RUNNING = false;
            return;
        }
        this.tmpEnd = Math.min(this.currentPos + this.stepSize, this.end);
        if (executeQuery()) {
            this.currentPos += super.totalSourceNum * this.stepSize;
        }
    }

    protected boolean executeQuery() {
        Boolean result = BasicDao.excuteQuery(this.dataSource, this.sql, this.handler);
        return (result != null && result);
    }

    protected String buildSQL() {
        return "SELECT " + this.columnNames + " FROM " + this.tableName + " WHERE " + this.primaryKeyName + " > ? AND " + this.primaryKeyName + " <= ? " + this.extraSQL;
    }

    @Override public void end() {
        super.end();
        Util.closeDataSource(this.dataSource);
    }
}
