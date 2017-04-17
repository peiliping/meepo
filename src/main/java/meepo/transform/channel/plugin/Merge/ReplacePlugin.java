package meepo.transform.channel.plugin.Merge;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import meepo.transform.channel.DataEvent;
import meepo.transform.channel.plugin.DefaultPlugin;
import meepo.transform.config.TaskContext;
import meepo.transform.report.IReportItem;
import meepo.transform.report.PluginReport;
import meepo.util.Constants;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import meepo.util.dao.ICallable;
import meepo.util.lrucache.LRUCache;
import org.apache.commons.lang3.tuple.Pair;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Created by peiliping on 17-4-15.
 */
public class ReplacePlugin extends DefaultPlugin {

    private DataSource dataSource;

    private String tableName;

    private int replacePosition;

    private String replaceFieldName;

    private int keyType;

    private String keyName;

    private String valName;

    private String sql;

    private ICallable<Object> handler;

    private Object tmpKey;

    private Object tmpVal;

    private LRUCache<Object> cache;

    private int cacheSize;

    private boolean Null4Null;

    private long metricReplaceByDB;

    public ReplacePlugin(TaskContext context) {
        super(context);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.replacePosition = context.getInteger("replacePosition", -1);
        this.replaceFieldName = context.getString("replaceFieldName");
        this.keyName = context.getString("keyName");
        this.valName = context.getString("valName");
        this.sql = "SELECT " + this.valName + " FROM " + this.tableName + " WHERE " + this.keyName + " = ?";
        this.handler = new ICallable<Object>() {
            @Override public void handleParams(PreparedStatement p) throws Exception {
                p.setObject(replacePosition, tmpKey, keyType);
            }

            @Override public Object handleResultSet(ResultSet r) throws Exception {
                return r.next() ? r.getObject(1) : null;
            }
        };
        this.cacheSize = context.getInteger("cacheSize", 0);
        this.cache = this.cacheSize > 0 ? new LRUCache<>(this.cacheSize, true) : null;
        this.Null4Null = context.getBoolean("null4null", true);
    }

    @Override public void convert(DataEvent de) {
        this.tmpKey = de.getSource()[this.replacePosition];
        if (this.cache == null) {
            this.metricReplaceByDB++;
            this.tmpVal = BasicDao.excuteQuery(this.dataSource, this.sql, this.handler);
        } else {
            this.tmpVal = this.cache.get(this.tmpKey, () -> {
                metricReplaceByDB++;
                return BasicDao.excuteQuery(dataSource, sql, handler);
            });
        }
        if (this.tmpVal == null) {
            this.tmpVal = this.Null4Null ? null : this.tmpKey;
        }
        de.getSource()[this.replacePosition] = this.tmpVal;
        super.convert(de);
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        if (this.replacePosition < 0) {
            source.forEach(i -> {
                if (this.replaceFieldName.equals(i.getLeft()))
                    this.replacePosition = source.indexOf(i);
            });
        }
        this.keyType = source.get(this.replacePosition).getRight();
        super.autoMatchSchema(source, sink);
    }

    @Override public void close() {
        Util.closeDataSource(this.dataSource);
        super.close();
    }

    @Override public IReportItem report() {
        return ReplacePluginReport.builder().name(this.getClass().getSimpleName() + "[" + this.tableName + "]").replaceByDB(this.metricReplaceByDB).build();
    }

    @Getter @Setter @Builder class ReplacePluginReport extends PluginReport {

        private String name;

        private long replaceByDB;
    }
}
