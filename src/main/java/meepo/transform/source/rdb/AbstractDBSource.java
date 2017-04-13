package meepo.transform.source.rdb;

import com.google.common.collect.Lists;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.transform.source.AbstractSource;
import meepo.transform.report.SourceReportItem;
import meepo.util.Constants;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import meepo.util.dao.ICallable;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.List;

/**
 * Created by peiliping on 17-3-11.
 */
public abstract class AbstractDBSource extends AbstractSource {

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

    public AbstractDBSource(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.primaryKeyName = context.getString("primaryKeyName", BasicDao.autoGetPrimaryKeyName(this.dataSource, this.tableName));
        this.columnNames = context.getString("columnNames", "*");
        this.extraSQL = context.getString("extraSQL", "");

        final List<String> columnsArray = Lists.newArrayList();
        super.schema = BasicDao.parserSchema(this.dataSource, this.tableName, this.columnNames, this.primaryKeyName);
        final boolean original = "*".equals(this.columnNames);
        if (original) {
            super.schema.forEach(item -> columnsArray.add("`" + item.getLeft() + "`"));
        } else {
            super.schema.forEach(item -> columnsArray.add(item.getLeft()));
        }
        this.columnNames = StringUtils.join(columnsArray, ",");
        super.columnsNum = super.schema.size();
        this.sql = buildSQL();
    }

    @Override public void work() {
        if (this.currentPos >= this.end) {
            super.RUNNING = false;
            return;
        }
        this.tmpEnd = Math.min(this.currentPos + this.stepSize, this.end);
        if (executeQuery()) {
            this.currentPos += super.totalSourceNum * this.stepSize;
        } else {
            Util.sleep(1);
        }
    }

    protected String buildSQL() {
        return "SELECT " + this.columnNames + " FROM " + this.tableName + " WHERE " + this.primaryKeyName + " > ? AND " + this.primaryKeyName + " <= ? " + this.extraSQL;
    }

    protected boolean executeQuery() {
        Boolean result = BasicDao.excuteQuery(this.dataSource, this.sql, this.handler);
        return (result != null && result);
    }

    @Override public void end() {
        super.end();
        Util.closeDataSource(this.dataSource);
    }

    @Override public SourceReportItem report() {
        return SourceReportItem.builder().name(this.taskName + "-Source-" + this.indexOfSources).start(this.start).current(this.currentPos).end(this.end).running(super.RUNNING)
                .build();
    }
}
