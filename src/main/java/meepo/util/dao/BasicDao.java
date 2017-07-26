package meepo.util.dao;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.google.common.collect.Lists;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by peiliping on 17-3-7.
 */
public class BasicDao {

    private static final Logger LOG = LoggerFactory.getLogger(BasicDao.class);

    public static DataSource createDataSource(TaskContext context) {
        try {
            Validate.notNull(context.get("url"));
            Validate.notNull(context.get("username"));
            Validate.notNull(context.get("password"));

            if (context.get("driverClassName") == null) {
                context.put("driverClassName", "com.mysql.jdbc.Driver");
            }
            if (context.get("initialSize") == null) {
                context.put("initialSize", "1");
            }
            if (context.get("minIdle") == null) {
                context.put("minIdle", "1");
            }
            if (context.get("maxActive") == null) {
                context.put("maxActive", "32");
            }
            if (context.get("defaultAutoCommit") == null) {
                context.put("defaultAutoCommit", "true");
            }
            if (context.get("timeBetweenEvictionRunsMillis") == null) {
                context.put("timeBetweenEvictionRunsMillis", "300000");
            }
            if (context.get("minEvictableIdleTimeMillis") == null) {
                context.put("minEvictableIdleTimeMillis", "300000");
            }
            if (context.get("validationQuery") == null) {
                context.put("validationQuery", "SELECT 'x' FROME DUAL");
            }
            if (context.get("testWhileIdle") == null) {
                context.put("testWhileIdle", "true");
            }
            if (context.get("testOnBorrow") == null) {
                context.put("testOnBorrow", "false");
            }
            if (context.get("testOnReturn") == null) {
                context.put("testOnReturn", "false");
            }
            if (context.get("poolPreparedStatements") == null) {
                context.put("poolPreparedStatements", "false");
            }
            if (context.get("maxPoolPreparedStatementPerConnectionSize") == null) {
                context.put("maxPoolPreparedStatementPerConnectionSize", "-1");
            }
            if (context.get("removeAbandoned") == null) {
                context.put("removeAbandoned", "true");
            }
            if (context.get("removeAbandonedTimeout") == null) {
                context.put("removeAbandonedTimeout", "1200");
            }
            if (context.get("logAbandoned") == null) {
                context.put("logAbandoned", "true");
            }
            return DruidDataSourceFactory.createDataSource(context.getParameters());
        } catch (Exception e) {
            LOG.error("Create DataSource Error : ", context.toString());
            Validate.isTrue(false);
        }
        return null;
    }

    public static void closeDataSource(DataSource ds) {
        try {
            if (ds instanceof DruidDataSource) {
                ((DruidDataSource) ds).close();
            } else if (ds instanceof Closeable) {
                ((Closeable) ds).close();
            }
        } catch (Exception e) {
            LOG.error("Close DataSource Error :", e);
        }
    }

    public static String autoGetPrimaryKeyName(DataSource ds, String dbName, String tableName) {
        String sql = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema='" + dbName + "' AND table_name='" + tableName + "' AND COLUMN_KEY='PRI'";
        return executeQuery(ds, sql, new ResultSetICallable<String>() {
            @Override
            public String handleResultSet(ResultSet r) throws Exception {
                Validate.isTrue(r.next());
                return r.getString(1);
            }
        });
    }

    public static String buildAutoGetStartEndSql(String tableName, String primaryKeyName) {
        return "SELECT " + "MIN(" + primaryKeyName + ") , MAX(" + primaryKeyName + ") FROM " + tableName;
    }

    public static Pair<Long, Long> autoGetStartEndPoint(DataSource ds, String tableName, String primaryKeyName, String sql) {
        if (sql == null) {
            sql = buildAutoGetStartEndSql(tableName, primaryKeyName);
        }
        return executeQuery(ds, sql, new ResultSetICallable<Pair<Long, Long>>() {
            @Override
            public Pair<Long, Long> handleResultSet(ResultSet r) throws Exception {
                Validate.isTrue(r.next());
                return Pair.of(r.getLong(1) - 1, r.getLong(2));
            }
        });
    }

    public static Pair<Long, Long> autoGetStartEndDatePoint(DataSource ds, String tableName, String primaryKeyName, String sql) {
        if (sql == null) {
            sql = buildAutoGetStartEndSql(tableName, primaryKeyName);
        }
        return executeQuery(ds, sql, new ResultSetICallable<Pair<Long, Long>>() {
            @Override
            public Pair<Long, Long> handleResultSet(ResultSet r) throws Exception {
                Validate.isTrue(r.next());
                return Pair.of(r.getTimestamp(1).getTime() - 1, r.getTimestamp(2).getTime());
            }
        });
    }

    public static List<Pair<String, Integer>> parserSchema(DataSource ds, String tableName, String columnNames) {
        String sql = "SELECT " + columnNames + " FROM " + tableName + " LIMIT 1";
        return executeQuery(ds, sql, new ResultSetICallable<List<Pair<String, Integer>>>() {
            @Override
            public List<Pair<String, Integer>> handleResultSet(ResultSet r) throws Exception {
                List<Pair<String, Integer>> result = Lists.newArrayList();
                Validate.isTrue(r.getMetaData().getColumnCount() > 0);
                for (int i = 1; i <= r.getMetaData().getColumnCount(); i++) {
                    String cn = r.getMetaData().getColumnName(i);
                    result.add(Pair.of(cn, r.getMetaData().getColumnType(i)));
                }
                return result;
            }
        });
    }

    public static <E> E executeQuery(DataSource ds, String sql, ICallable<E> cal) {
        Connection c = null;
        PreparedStatement p = null;
        try {
            c = ds.getConnection();
            p = c.prepareStatement(sql);
            cal.handleParams(p);
            ResultSet r = p.executeQuery();
            E e = cal.handleResultSet(r);
            r.close();
            return e;
        } catch (Exception e) {
            LOG.error("basicdao.executequery", e);
            Validate.isTrue(false);
        } finally {
            try {
                if (p != null)
                    p.close();
                if (c != null)
                    c.close();
            } catch (SQLException e) {
                LOG.error("basicdao.executequery", e);
            }
        }
        return null;
    }

    public static String matchDBType(TaskContext context) {
        Validate.notNull(context.get("url"));
        Pattern pattern = Pattern.compile("jdbc:(.+)://(.+)/(.+)\\?(.+)");
        Matcher m = pattern.matcher(context.get("url"));
        if (m.find())
            return m.group(1);
        Validate.isTrue(false, "no dbtype info .");
        return null;
    }

    public static String matchDBName(TaskContext context) {
        Validate.notNull(context.get("url"));
        Pattern pattern = Pattern.compile("jdbc:(.+)://(.+)/(.+)\\?(.+)");
        Matcher m = pattern.matcher(context.get("url"));
        if (m.find())
            return m.group(3);
        Validate.isTrue(false, "no dbname info .");
        return null;
    }

}
