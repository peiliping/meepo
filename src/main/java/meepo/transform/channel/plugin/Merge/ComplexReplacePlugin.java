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
import java.util.Arrays;
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

    private List<Integer> filterPositions = Lists.newArrayList();

    private List<String> filterFieldNames = Lists.newArrayList();

    private List<Integer> keyTypes = Lists.newArrayList();

    private List<String> keyNames = Lists.newArrayList();

    private List<String> valNames = Lists.newArrayList();

    private String sql;

    private LRUCache<Object[]> cache;

    private int cacheSize;

    private boolean Null4Null;

    private AtomicLong metricReplaceByDB = new AtomicLong(0);

    public ComplexReplacePlugin(TaskContext context) {
        super(context);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");

        if (StringUtils.isNotBlank(context.getString("replacePositions"))) {
            List<String> rps = Lists.newArrayList(context.getString("replacePositions").split("\\s"));
            rps.forEach(k -> replacePositions.add(Integer.valueOf(k)));
        }
        this.replaceFieldNames.addAll(Lists.newArrayList(context.getString("replaceFieldNames", "").split("\\s")));

        if (StringUtils.isNotBlank(context.getString("filterPositions"))) {
            List<String> fps = Lists.newArrayList(context.getString("filterPositions").split("\\s"));
            fps.forEach(k -> filterPositions.add(Integer.valueOf(k)));
        }
        this.filterFieldNames.addAll(Lists.newArrayList(context.getString("filterFieldNames", "").split("\\s")));

        this.keyNames.addAll(Lists.newArrayList(context.getString("keyNames").split("\\s")));
        this.valNames.addAll(Lists.newArrayList(context.getString("valNames").split("\\s")));
        this.sql = "SELECT " + StringUtils.join(this.valNames, ",") + " FROM " + this.tableName + " WHERE " + StringUtils.join(this.keyNames, " = ? AND ") + " = ? ";
        this.cacheSize = context.getInteger("cacheSize", 0);
        this.cache = this.cacheSize > 0 ? new LRUCache<>(this.cacheSize, true) : null;
        this.Null4Null = context.getBoolean("null4null", true);
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        if (this.replacePositions.isEmpty()) {
            this.replaceFieldNames.forEach(s -> source.forEach(i -> {
                if (s.equals(i.getLeft())) {
                    replacePositions.add(source.indexOf(i));
                }
            }));
        }
        if (this.filterPositions.isEmpty()) {
            this.filterFieldNames.forEach(s -> source.forEach(i -> {
                if (s.equals(i.getLeft())) {
                    filterPositions.add(source.indexOf(i));
                }
            }));
        }
        this.filterPositions.forEach(i -> keyTypes.add(source.get(i).getRight()));
        super.autoMatchSchema(source, sink);
    }

    @Override public void convert(DataEvent de, boolean theEnd) {
        Object[] filters = new Object[this.filterPositions.size()];
        for (int i = 0; i < this.filterPositions.size(); i++) {
            filters[i] = de.getSource()[this.filterPositions.get(i)];
        }

        ICallable<Object[]> handler = new ICallable<Object[]>() {
            @Override public void handleParams(PreparedStatement p) throws Exception {
                for (int i = 0; i < filters.length; i++)
                    p.setObject(i + 1, filters[i], keyTypes.get(i));
            }

            @Override public Object[] handleResultSet(ResultSet r) throws Exception {
                if (r.next()) {
                    Object[] vals = new Object[valNames.size()];
                    for (int i = 0; i < vals.length; i++) {
                        vals[i] = r.getObject(i + 1);
                    }
                    return vals;
                } else {
                    return null;
                }
            }
        };

        Object[] vals = null;
        boolean result = true;
        if (this.cache == null) {
            this.metricReplaceByDB.incrementAndGet();
            vals = BasicDao.executeQuery(this.dataSource, this.sql, handler);
        } else {
            String key = Arrays.toString(filters);
            vals = this.cache.get(key, () -> {
                this.metricReplaceByDB.incrementAndGet();
                return BasicDao.executeQuery(dataSource, sql, handler);
            });
        }
        if (vals == null) {
            result = false;
            vals = new Object[this.valNames.size()];
        }

        if (result || this.Null4Null) {
            for (int i = 0; i < this.replacePositions.size(); i++) {
                de.getSource()[this.replacePositions.get(i)] = vals[i];
            }
        }
        super.convert(de, theEnd);
    }

    @Override public void close() {
        Util.closeDataSource(this.dataSource);
        super.close();
    }

    @Override public IReportItem report() {
        return ReplacePluginReport.builder().name(this.getClass().getSimpleName() + "[" + this.tableName + "]").replaceByDB(this.metricReplaceByDB.get()).build();
    }

}
