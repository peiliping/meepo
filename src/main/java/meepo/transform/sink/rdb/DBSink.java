package meepo.transform.sink.rdb;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.util.Constants;
import meepo.util.DataSourceCache;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Created by peiliping on 17-3-7.
 */
public class DBSink extends AbstractSink {

    protected DataSource dataSource;

    protected String tableName;

    protected String primaryKeyName;

    protected int stepSize;

    protected String columnNames;

    protected String paramsStr;// ?,?,?,?

    protected String sql;

    protected Handler handler;

    protected long count;

    protected long lastCommit;

    protected long lastFlushTS;

    protected boolean sinkSharedDataSource;

    public DBSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.sinkSharedDataSource = context.getBoolean("sharedDatasource", true);
        this.dataSource = this.sinkSharedDataSource ?
                DataSourceCache.createDataSource(name + "-sink", new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_))) :
                Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.primaryKeyName = context.getString("primaryKeyName", BasicDao.autoGetPrimaryKeyName(this.dataSource, this.tableName));
        this.stepSize = context.getInteger("stepSize", 100);
        this.columnNames = context.getString("columnNames", "*");

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

    @Override public void onEvent(Object event) throws Exception {
        super.metricCount++;
        this.handler.prepare();
        super.RUNNING = true;
        this.handler.feed((DataEvent) event);
        this.count++;
        if (this.count - this.lastCommit >= this.stepSize) {
            sinkFlush();
        } else if (this.lastFlushTS - System.currentTimeMillis() > 3000) {
            sinkFlush();
        }
    }

    @Override public void timeOut() {
        sinkFlush();
    }

    private void sinkFlush() {
        if (this.count - this.lastCommit > 0) {
            super.metricBatchCount++;
            if (this.count - this.lastCommit < this.stepSize) {
                super.metricUnsaturatedBatch++;
            }
            this.handler.flush();
            super.RUNNING = false;
            this.lastCommit = this.count;
            this.lastFlushTS = System.currentTimeMillis();
        }
    }

    @Override public void onShutdown() {
        timeOut();
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

        void prepare() {
            try {
                if (c == null || c.isClosed() || p == null || p.isClosed()) {
                    c = dataSource.getConnection();
                    c.setAutoCommit(false);
                    p = c.prepareStatement(sql);
                }
            } catch (Exception e) {
                LOG.error("DBSink-Handler-Prepare Error :", e);
                c = null;
                p = null;
                prepare();
            }
        }

        void feed(DataEvent de) {
            try {
                for (int i = 0; i < de.getTarget().length; i++) {
                    p.setObject(i + 1, de.getTarget()[i], schema.get(i).getRight());
                }
                p.addBatch();
            } catch (Exception e) {
                LOG.error("DBSink-Handler-Feed Error :" + de.toString(), e);
                Util.sleep(1);
                feed(de);
            }
        }

        void flush() {
            try {
                p.executeBatch();
                c.commit();
            } catch (Exception e) {
                LOG.error("DBSink-Handler-Flush Error :", e);
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
