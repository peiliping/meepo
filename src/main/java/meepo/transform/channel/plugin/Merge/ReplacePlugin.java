package meepo.transform.channel.plugin.Merge;

import meepo.transform.channel.DataEvent;
import meepo.transform.channel.plugin.DefaultPlugin;
import meepo.transform.config.TaskContext;
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

    private int keyPosition;

    private int keyType;

    private String keyName;

    private String valName;

    private String sql;

    private ICallable<Object> handler;

    private Object tmp;

    private LRUCache<Object> cache;

    private int cacheSize;

    public ReplacePlugin(TaskContext context) {
        super(context);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
        this.tableName = context.getString("tableName");
        this.keyPosition = context.getInteger("keyPosition");
        this.keyName = context.getString("keyName");
        this.valName = context.getString("valName");
        this.sql = "SELECT " + this.valName + " FROM " + this.tableName + " WHERE " + this.keyName + " = ?";
        this.handler = new ICallable<Object>() {
            @Override public void handleParams(PreparedStatement p) throws Exception {
                p.setObject(keyPosition, tmp, keyType);
            }

            @Override public Object handleResultSet(ResultSet r) throws Exception {
                return r.next() ? r.getObject(1) : null;
            }
        };
        this.cacheSize = context.getInteger("cacheSize", 0);
        this.cache = this.cacheSize > 0 ? new LRUCache<>(this.cacheSize, true) : null;
    }

    @Override public void convert(DataEvent de) {
        this.tmp = de.getSource()[this.keyPosition];
        if (this.cache == null) {
            de.getSource()[this.keyPosition] = BasicDao.excuteQuery(this.dataSource, this.sql, this.handler);
        } else {
            de.getSource()[this.keyPosition] = this.cache.get(this.tmp, () -> BasicDao.excuteQuery(dataSource, sql, handler));
        }
        super.convert(de);
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        this.keyType = source.get(this.keyPosition).getRight();
        super.autoMatchSchema(source, sink);
    }

    @Override public void close() {
        Util.closeDataSource(this.dataSource);
        super.close();
    }
}
