package meepo.util.dao;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by peiliping on 17-3-7.
 */
public class BasicDao {

    private static final Logger LOG = LoggerFactory.getLogger(BasicDao.class);

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

    public static List<Pair<String, Integer>> parserSchema(DataSource ds, String tableName, String columnNames, String primaryKeyName) {
        String sql = "SELECT " + columnNames + " FROM " + tableName + " WHERE " + primaryKeyName + " = 0" + " LIMIT 1";
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

}
