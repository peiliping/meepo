package meepo.transform.channel.plugin.Merge;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.channel.plugin.DefaultPlugin;
import meepo.transform.config.TaskContext;
import meepo.transform.report.IReportItem;
import meepo.util.Constants;
import meepo.util.Util;
import meepo.util.dao.BasicDao;
import meepo.util.dao.ICallable;
import meepo.util.lrucache.LRUCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by peiliping on 17-4-15.
 */
public class ComplexReplacePlugin extends DefaultPlugin {

    private DataSource dataSource;

    private String tableName;

    private List<Integer> replacePositions = Lists.newArrayList();

    private List<String> replaceFieldNames = Lists.newArrayList();

    private int[] keyType;

    private List<String> keyNames = Lists.newArrayList();

    private List<String> valNames = Lists.newArrayList();

    private String sql;

    private ICallable<Object> handler;

    private Object[] tmpKey;

    private Object[] tmpVal;

    private LRUCache<Object> cache;

    private int cacheSize;

    private boolean Null4Null;

    private AtomicLong metricReplaceByDB = new AtomicLong(0);

    public ComplexReplacePlugin(TaskContext context) {
        super(context);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        List<String> rps = Lists.newArrayList(context.getString("replacePositions", "").split("\\s"));
        rps.forEach(k -> replacePositions.add(Integer.valueOf(k)));
        this.replaceFieldNames.addAll(Lists.newArrayList(context.getString("replaceFieldNames", "").split("\\s")));
        this.keyNames.addAll(Lists.newArrayList(context.getString("keyNames").split("\\s")));
        this.valNames.addAll(Lists.newArrayList(context.getString("valNames").split("\\s")));
        this.sql = "SELECT " + StringUtils.join(this.keyNames, ",") + " FROM " + this.tableName + " WHERE " + StringUtils.join(valNames, " AND ");

        //        this.handler = new ICallable<Object>() {
        //            @Override public void handleParams(PreparedStatement p) throws Exception {
        //                p.setObject(1, tmpKey, keyType);
        //            }
        //
        //            @Override public Object handleResultSet(ResultSet r) throws Exception {
        //                return r.next() ? r.getObject(1) : null;
        //            }
        //        };
        this.cacheSize = context.getInteger("cacheSize", 0);
        this.cache = this.cacheSize > 0 ? new LRUCache<>(this.cacheSize, true) : null;
        this.Null4Null = context.getBoolean("null4null", true);
    }

    @Override public void convert(DataEvent de) {
        //        this.tmpKey = de.getSource()[this.replacePosition];
        //        if (this.cache == null) {
        //            this.metricReplaceByDB++;
        //            this.tmpVal = BasicDao.excuteQuery(this.dataSource, this.sql, this.handler);
        //        } else {
        //            this.tmpVal = this.cache.get(this.tmpKey, () -> {
        //                metricReplaceByDB++;
        //                return BasicDao.excuteQuery(dataSource, sql, handler);
        //            });
        //        }
        //        if (this.tmpVal == null) {
        //            this.tmpVal = this.Null4Null ? null : this.tmpKey;
        //        }
        //        de.getSource()[this.replacePosition] = this.tmpVal;
        super.convert(de);
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        //        if (this.replacePosition < 0) {
        //            source.forEach(i -> {
        //                if (this.replaceFieldName.equals(i.getLeft()))
        //                    this.replacePosition = source.indexOf(i);
        //            });
        //        }
        //        this.keyType = source.get(this.replacePosition).getRight();
        //        super.autoMatchSchema(source, sink);
    }

    @Override public void close() {
        Util.closeDataSource(this.dataSource);
        super.close();
    }

    @Override public IReportItem report() {
        return ReplacePluginReport.builder().name(this.getClass().getSimpleName() + "[" + this.tableName + "]").replaceByDB(this.metricReplaceByDB.get()).build();
    }

}
