package meepo.transform.sink.rdb;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import meepo.transform.sink.batch.IHandler;
import meepo.transform.sink.rdb.handlers.MysqlHandler;
import meepo.util.Constants;
import meepo.util.DataSourceCache;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
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

    protected IHandler handler;

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
        this.handler = new MysqlHandler(this.dataSource, this.sql, super.schema);
        this.lastFlushTS = System.currentTimeMillis();
    }

    @Override public void onStart() {
        super.onStart();
        this.handler.init();
        if (this.truncateTable && super.indexOfSinks == 0) {
            this.handler.truncate(this.tableName);
        }
    }

    @Override public void onEvent(Object event) throws Exception {
        super.RUNNING = true;
        this.handler.prepare(this.stepSize);
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
            this.handler.close();
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
}
