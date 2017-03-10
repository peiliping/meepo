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
 * Created by peiliping on 17-3-10.
 */
public class DBSyncByDateSource extends AbstractSource {

    private DataSource dataSource;

    private String tableName;

    private String primaryKeyName;

    private int stepSize;

    private long delay;

    private String columnNames;

    private String extraSQL;

    private long now;

    private long start;

    private Pair<Long, Long> startEnd;

    private long currentPos;

    private long tmpEnd;

    private String sql;

    private ICallable<Boolean> handler;

    public DBSyncByDateSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
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
        this.delay = context.getLong("delay", 5000L);
        this.now = System.currentTimeMillis();
        this.start = context.getLong("start", this.now - this.delay);
        this.currentPos = this.start;

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
        this.startEnd = BasicDao.autoGetStartEndDatePoint(this.dataSource, this.tableName, this.primaryKeyName);
        this.now = System.currentTimeMillis();
        this.tmpEnd = Math.min(this.currentPos + this.stepSize, Math.min(this.now - this.delay, this.startEnd.getRight()));
        if (this.tmpEnd == this.currentPos) {
            this.currentPos = this.now - this.delay;
            Util.sleep(1);
            return;
        }
        if (executeQuery()) {
            this.currentPos = this.tmpEnd;
        }
    }

    private boolean executeQuery() {
        Boolean result = BasicDao.excuteQuery(this.dataSource, this.sql, this.handler);
        return (result != null && result);
    }

    private String buildSQL() {
        return "SELECT " + this.columnNames + " FROM " + this.tableName + " WHERE " + this.primaryKeyName + " > ? AND " + this.primaryKeyName + " <= ? " + this.extraSQL;
    }

    @Override public void end() {
        super.end();
        Util.closeDataSource(this.dataSource);
    }
}
