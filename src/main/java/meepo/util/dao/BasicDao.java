package meepo.util.dao;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by peiliping on 17-3-7.
 */
public class BasicDao {

    protected static final Logger LOG = LoggerFactory.getLogger(BasicDao.class);

    public static String autoGetPrimaryKeyName(DataSource ds, String tableName) {
        String sql = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_name='" + tableName + "' AND COLUMN_KEY='PRI'";
        return excuteQuery(ds, sql, new ResultSetICallable<String>() {
            @Override public String handleResultSet(ResultSet r) throws Exception {
                Validate.isTrue(r.next());
                return r.getString(1);
            }
        });
    }

    public static Pair<Long, Long> autoGetStartEndPoint(DataSource ds, String tableName, String primaryKeyName) {
        String sql = "SELECT " + "MIN(" + primaryKeyName + ") , MAX(" + primaryKeyName + ") FROM " + tableName;
        return excuteQuery(ds, sql, new ResultSetICallable<Pair<Long, Long>>() {
            @Override public Pair<Long, Long> handleResultSet(ResultSet r) throws Exception {
                Validate.isTrue(r.next());
                return Pair.of(r.getLong(1) - 1, r.getLong(2));
            }
        });
    }

    public static List<Pair<String, Integer>> parserSchema(DataSource ds, String tableName, String columNames, String primaryKeyName) {
        String sql = "SELECT " + columNames + " FROM " + tableName + " WHERE " + primaryKeyName + " = 0" + " LIMIT 1";
        return excuteQuery(ds, sql, new ResultSetICallable<List<Pair<String, Integer>>>() {
            @Override public List<Pair<String, Integer>> handleResultSet(ResultSet r) throws Exception {
                List<Pair<String, Integer>> result = Lists.newArrayList();
                Validate.isTrue(r.getMetaData().getColumnCount() > 0);
                for (int i = 1; i <= r.getMetaData().getColumnCount(); i++) {
                    result.add(Pair.of(r.getMetaData().getColumnName(i), r.getMetaData().getColumnType(i)));
                }
                return result;
            }
        });
    }

    public static <E> E excuteQuery(DataSource ds, String sql, ICallable<E> cal) {
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
            LOG.error("basicdao.excutequery", e);
        } finally {
            try {
                if (p != null)
                    p.close();
                if (c != null)
                    c.close();
            } catch (SQLException e) {
                LOG.error("basicdao.excutequery", e);
            }
        }
        return null;
    }

    public static <E> boolean excuteBatchAdd(DataSource ds, String sql, ICallable<E> cal) {
        Connection c = null;
        PreparedStatement p = null;
        try {
            c = ds.getConnection();
            c.setAutoCommit(false);
            p = c.prepareStatement(sql);
            cal.handleParams(p);
            if (p.isClosed()) {
                return excuteBatchAdd(ds, sql, cal);
            }
            p.executeBatch();
            c.commit();
            return true;
        } catch (Exception e) {
            LOG.error("basicdao.excuteBatchAdd", e);
            return false;
        } finally {
            try {
                if (c != null)
                    c.close();
            } catch (SQLException e) {
                LOG.error("basicdao.excuteBatchAdd", e);
            }
        }
    }

    public static <E> boolean excuteLoadData(DataSource ds, String sql, InputStream data) {
        Connection c = null;
        PreparedStatement p = null;
        int result = 0;
        try {
            c = ds.getConnection();
            p = c.prepareStatement(sql);
            if (p.isWrapperFor(com.mysql.jdbc.Statement.class)) {
                com.mysql.jdbc.PreparedStatement mysqlStatement = p.unwrap(com.mysql.jdbc.PreparedStatement.class);
                mysqlStatement.setLocalInfileInputStream(data);
                result = mysqlStatement.executeUpdate();
            }
        } catch (Exception e) {
            LOG.error("basicdao.excuteLoadData", e);
        } finally {
            try {
                if (p != null)
                    p.close();
                if (c != null)
                    c.close();
            } catch (SQLException e) {
                LOG.error("basicdao.excutequery", e);
            }
        }
        return result > 0;
    }

}
