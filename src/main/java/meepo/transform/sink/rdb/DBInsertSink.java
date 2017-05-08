package meepo.transform.sink.rdb;

import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import com.google.common.collect.Lists;
import com.mysql.jdbc.JDBC4PreparedStatement;
import com.mysql.jdbc.StatementImpl;
import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.Constants;
import meepo.util.DataSourceCache;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import meepo.util.date.DateFormatter;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by peiliping on 17-3-7.
 */
public class DBInsertSink extends AbstractSink {

    protected DataSource dataSource;

    protected String tableName;

    protected String primaryKeyName;

    protected int stepSize;

    protected String columnNames;

    protected String paramsStr;// ?,?,?,?

    protected String sql;

    protected Handler handler;

    protected long lastCommit;

    protected long lastFlushTS;

    protected boolean sinkSharedDataSource;

    protected boolean truncateTable;

    public DBInsertSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.sinkSharedDataSource = context.getBoolean("sharedDatasource", true);
        this.dataSource = this.sinkSharedDataSource ?
                DataSourceCache.createDataSource(name + "-sink", new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_))) :
                Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.primaryKeyName = context.getString("primaryKeyName", BasicDao.autoGetPrimaryKeyName(this.dataSource, this.tableName));
        this.stepSize = context.getInteger("stepSize", 100);
        this.columnNames = context.getString("columnNames", "*");
        this.truncateTable = context.getBoolean("truncate", false);

        super.schema = BasicDao.parserSchema(this.dataSource, this.tableName, this.columnNames, this.primaryKeyName);
        final List<String> columnsArray = Lists.newArrayList();
        final List<String> paramsArray = Lists.newArrayList();
        final boolean original = "*".equals(this.columnNames);
        super.schema.forEach(item -> {
            paramsArray.add("?");
            if (original) {
                columnsArray.add("`" + item.getLeft() + "`");
            } else {
                columnsArray.add(item.getLeft());
            }
        });
        this.columnNames = StringUtils.join(columnsArray, ",");
        this.paramsStr = StringUtils.join(paramsArray, ",");

        this.sql = buildSQL();
        this.handler = new Handler();
        this.lastFlushTS = System.currentTimeMillis();
    }

    @Override public void onStart() {
        super.onStart();
        this.handler.init();
    }

    @Override public void onEvent(Object event) throws Exception {
        super.RUNNING = true;
        this.handler.prepare();
        super.count++;
        this.handler.feed((DataEvent) event);
        if (super.count - this.lastCommit >= this.stepSize || System.currentTimeMillis() - this.lastFlushTS > 3000) {
            sinkFlush();
        }
    }

    @Override public void timeOut() {
        sinkFlush();
    }

    private void sinkFlush() {
        if (super.count - this.lastCommit > 0) {
            super.metricBatchCount++;
            if (super.count - this.lastCommit < this.stepSize) {
                super.metricUnsaturatedBatch++;
            }
            this.handler.flush();
            super.RUNNING = false;
            this.lastCommit = super.count;
        } else {
            this.handler.justClose();
        }
        this.lastFlushTS = System.currentTimeMillis();
    }

    @Override public void onShutdown() {
        sinkFlush();
        if (this.sinkSharedDataSource) {
            DataSourceCache.close(super.taskName + "-sink");
        } else {
            Util.closeDataSource(this.dataSource);
        }
        super.onShutdown();
    }

    public String buildSQL() {
        return "INSERT INTO " + this.tableName + " (" + this.columnNames + ") VALUES ( " + this.paramsStr + ")";
    }

    class Handler {

        private Connection c = null;

        private PreparedStatement p = null;

        private Field batchArgsField;

        private Field tsdf;

        void init() {
            try {
                batchArgsField = StatementImpl.class.getDeclaredField("batchedArgs");
                batchArgsField.setAccessible(true);
            } catch (Exception e) {
            }
            try {
                tsdf = com.mysql.jdbc.PreparedStatement.class.getDeclaredField("tsdf");
                tsdf.setAccessible(true);
            } catch (Exception e) {
            }

            if (truncateTable && indexOfSinks == 0) {
                try {
                    Connection con = dataSource.getConnection();
                    Statement st = con.createStatement();
                    st.execute("truncate table " + tableName);
                    st.close();
                    con.close();
                } catch (Throwable e) {
                    LOG.error("Truncate Table [" + tableName + "] Failed .");
                }
            }
        }

        void prepare() {
            try {
                if (c == null || p == null) {
                    c = dataSource.getConnection();
                    c.setAutoCommit(false);
                    p = c.prepareStatement(sql);
                    if (batchArgsField != null && ((DruidPooledPreparedStatement) p).getStatement() instanceof StatementImpl) {
                        batchArgsField.set((StatementImpl) ((DruidPooledPreparedStatement) p).getStatement(), new ArrayList<Object>(stepSize));
                        tsdf.set(((com.mysql.jdbc.PreparedStatement) ((DruidPooledPreparedStatement) p).getStatement()), new DateFormatter("''yyyy-MM-dd HH:mm:ss"));
                    }
                }
            } catch (Exception e) {
                LOG.error("DBInsertSink-Handler-Prepare Error :", e);
                c = null;
                p = null;
                Util.sleep(1);
                prepare();
            }
        }

        void feed(DataEvent de) {
            try {
                for (int i = 0; i < schema.size(); i++) {
                    p.setObject(i + 1, de.getTarget()[i], schema.get(i).getRight());
                }
                p.addBatch();
            } catch (Exception e) {
                LOG.error("DBInsertSink-Handler-Feed Error :", e);
                LOG.error("Data :", de.toString());
                Util.sleep(1);
                feed(de);
            }
        }

        void flush() {
            try {
                p.executeBatch();
                c.commit();
            } catch (Exception e) {
                LOG.error("DBInsertSink-Handler-Flush Error :", e);
                while (retry()) {
                    Util.sleep(1);
                }
            } finally {
                try {
                    if (p != null)
                        p.close();
                    if (c != null)
                        c.close();
                } catch (Exception e) {
                    LOG.error("DBInsertSink-Handler-Flush Error :", e);
                }
                c = null;
                p = null;
            }
        }

        boolean retry() {
            try {
                List<Object> batchParams = ((JDBC4PreparedStatement) ((DruidPooledPreparedStatement) p).getStatement()).getBatchedArgs();
                Connection tc = dataSource.getConnection();
                tc.setAutoCommit(false);
                PreparedStatement tp = tc.prepareStatement(sql);
                StatementImpl target = (StatementImpl) ((DruidPooledPreparedStatement) tp).getStatement();
                List<Object> newParams = new ArrayList<Object>(batchParams);
                batchArgsField.set(target, newParams);
                tp.executeBatch();
                tc.commit();
                tp.close();
                tc.close();
            } catch (Throwable e) {
                LOG.error("DBInsertSink-Handler-Retry Error :", e);
                return false;
            }
            return true;
        }

        void justClose() {
            try {
                if (p != null)
                    p.close();
                if (c != null)
                    c.close();
            } catch (Exception e) {
                LOG.error("DBInsertSink-Handler-JustClose Error :", e);
            }
            c = null;
            p = null;
        }
    }
}
