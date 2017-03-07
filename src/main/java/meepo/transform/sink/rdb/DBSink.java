package meepo.transform.sink.rdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.Constants;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

/**
 * Created by peiliping on 17-3-7.
 */
public class DBSink extends AbstractSink {

    protected DataSource dataSource;

    protected String tableName;

    protected String primaryKeyName;

    protected int stepSize;

    protected String columnNames;

    protected List<String> columnsArray = Lists.newArrayList();

    protected List<Integer> typesArray = Lists.newArrayList();

    protected Map<String, Integer> columnsType = Maps.newHashMap();

    protected int columnsNum;

    protected String sql;

    protected long count;

    protected long lastcommit;

    protected Handler handler;

    public DBSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.primaryKeyName = context.getString("primaryKeyName", BasicDao.autoGetPrimaryKeyName(this.dataSource, this.tableName));
        this.stepSize = context.getInteger("stepSize", 100);
        this.columnNames = context.getString("columnNames", "*");

        Triple<List<String>, List<Integer>, Map<String, Integer>> schema = BasicDao.parserSchema(this.dataSource, this.tableName, this.columnNames, this.primaryKeyName);
        this.columnsArray.addAll(schema.getLeft());
        this.typesArray.addAll(schema.getMiddle());
        this.columnsType.putAll(schema.getRight());
        this.columnNames = StringUtils.join(columnsArray, ",");
        this.columnsNum = this.columnsArray.size();

        this.sql = buildSQL();

        this.handler = new Handler();
    }

    @Override public void onEvent(Object event) throws Exception {
        DataEvent de = (DataEvent) event;
        this.handler.prepare();
        this.handler.feed(de);
        this.count++;
        if (this.count - this.lastcommit >= this.stepSize) {
            this.handler.flush();
            this.lastcommit = this.count;
        }
    }

    @Override public void timeOut() {
        if (this.count - this.lastcommit > 0) {
            this.handler.flush();
            this.lastcommit = this.count;
        }
    }

    @Override public void onShutdown() {
        super.onShutdown();
        Util.closeDataSource(this.dataSource);
    }

    public String buildSQL() {
        String v = "?";
        for (int i = 1; i < this.columnsNum; i++) {
            v = v + ",?";
        }
        return "INSERT INTO " + this.tableName + " (" + this.columnNames + ") VALUES ( " + v + ")";
    }

    class Handler {

        private Connection c = null;

        private PreparedStatement p = null;

        void prepare() {
            try {
                if (c == null || c.isClosed() || p == null || p.isClosed()) {
                    c = dataSource.getConnection();
                    c.setAutoCommit(false);
                    p = c.prepareStatement(sql);
                }
            } catch (Exception e) {
                LOG.error("DBSink-Handler-Prepare Error :", e);
                try {
                    p.close();
                    c.close();
                } catch (Exception e2) {
                    LOG.error("DBSink-Handler-Prepare Error :", e2);
                }
                c = null;
                p = null;
            }
        }

        void feed(DataEvent de) {
            try {
                for (int i = 0; i < de.getSource().length; i++) {
                    p.setObject(i + 1, de.getSource()[i], typesArray.get(i));
                }
                p.addBatch();
            } catch (Exception e) {
                LOG.error("Data Error : " + de.toString(), e);
            }
        }

        void flush() {
            try {
                p.executeBatch();
                c.commit();
            } catch (Exception e) {
                LOG.error("Data Error : ", e);

            } finally {
                try {
                    p.close();
                    c.close();
                } catch (Exception e) {
                    LOG.error("DBSink-Handler-Flush Error :", e);
                }
                c = null;
                p = null;
            }
        }
    }
}
