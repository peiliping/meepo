package meepo.transform.source.rdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.apache.commons.lang3.tuple.Triple;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/**
 * Created by peiliping on 17-3-7.
 */
public class DBSource extends AbstractSource {

    protected DataSource dataSource;

    protected String tableName;

    protected String primaryKeyName;

    protected int stepSize;

    protected String columnNames;

    protected List<String> columnsArray = Lists.newArrayList();

    protected List<Integer> typesArray = Lists.newArrayList();

    protected Map<String, Integer> columnsType = Maps.newHashMap();

    protected int columnsNum;

    protected String extraSQL;

    protected long currentPos = 0;

    protected Long start;

    protected Long end;

    protected String sql;

    public DBSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.primaryKeyName = context.getString("primaryKeyName", BasicDao.autoGetPrimaryKeyName(this.dataSource, this.tableName));
        this.stepSize = context.getInteger("stepSize", 100);
        this.columnNames = context.getString("columnNames", "*");
        this.extraSQL = context.getString("extraSQL", "");

        Triple<List<String>, List<Integer>, Map<String, Integer>> schema = BasicDao.parserSchema(this.dataSource, this.tableName, this.columnNames, this.primaryKeyName);
        this.columnsArray.addAll(schema.getLeft());
        this.typesArray.addAll(schema.getMiddle());
        this.columnsType.putAll(schema.getRight());
        this.columnNames = StringUtils.join(columnsArray, ",");
        this.columnsNum = this.columnsArray.size();

        Pair<Long, Long> ps = BasicDao.autoGetStartEndPoint(this.dataSource, this.tableName, this.primaryKeyName);
        this.start = context.getLong("start", ps.getLeft());
        this.end = context.getLong("end", ps.getRight());
        long vStart = start - (start % this.stepSize);
        this.currentPos = Math.max(vStart + index * this.stepSize, start);

        this.sql = buildSQL();
    }

    @Override public Object[] eventFactory() {
        return new Object[this.columnsNum];
    }

    @Override public void work() {
        if (this.currentPos >= this.end) {
            super.RUNNING = false;
            return;
        }
        boolean status = executeQuery(this.currentPos, Math.min(this.currentPos + this.stepSize, this.end));
        if (status) {
            currentPos += super.totalSourceNum * this.stepSize;
        }
    }

    protected String buildSQL() {
        return "SELECT " + this.columnNames + " FROM " + this.tableName + " WHERE " + this.primaryKeyName + " > ? AND " + this.primaryKeyName + " <= ? " + this.extraSQL;
    }

    protected boolean executeQuery(final long start, final long end) {
        Boolean result = BasicDao.excuteQuery(this.dataSource, sql, new ICallable<Boolean>() {
            @Override public void handleParams(PreparedStatement p) throws Exception {
                p.setLong(1, start);
                p.setLong(2, end);
            }

            @Override public Boolean handleResultSet(ResultSet r) throws Exception {
                while (r.next()) {
                    DataEvent de = feedOne();
                    for (int i = 1; i <= columnsArray.size(); i++) {
                        de.getSource()[i - 1] = r.getObject(i);
                    }
                    channel.pushBySeq(tmpIndex);
                }
                return true;
            }
        });
        return (result != null && result);
    }
}
