package meepo.transform.sink.rdb.handlers;

import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import com.mysql.jdbc.JDBC4PreparedStatement;
import com.mysql.jdbc.StatementImpl;
import meepo.transform.channel.DataEvent;
import meepo.transform.sink.batch.IHandler;
import meepo.util.Util;
import meepo.util.date.DateFormatter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by peiliping on 17-5-11.
 */
public class MysqlHandler implements IHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(MysqlHandler.class);

    protected DataSource dataSource;

    protected String sql;

    protected Connection connection;

    protected PreparedStatement preparedStatement;

    protected Field batchArgsField;

    protected Field tsdf;

    protected List<Pair<String, Integer>> schema;

    public MysqlHandler(DataSource ds, String sql, List<Pair<String, Integer>> schema) {
        this.dataSource = ds;
        this.sql = sql;
        this.schema = schema;
    }

    @Override public void init() {
        try {
            this.batchArgsField = StatementImpl.class.getDeclaredField("batchedArgs");
            this.batchArgsField.setAccessible(true);
        } catch (Exception e) {
            LOG.error("Init batchArgsField Failed : ", e);
        }
        try {
            this.tsdf = com.mysql.jdbc.PreparedStatement.class.getDeclaredField("tsdf");
            this.tsdf.setAccessible(true);
        } catch (Exception e) {
            LOG.error("Init tsdf Failed : ", e);
        }
    }

    @Override public void truncate(String tableName) {
        try {
            Connection con = this.dataSource.getConnection();
            Statement st = con.createStatement();
            st.execute("truncate table " + tableName);
            st.close();
            con.close();
        } catch (Throwable e) {
            LOG.error("Truncate Table [" + tableName + "] Failed .");
        }
    }

    @Override public void prepare(int stepSize) {
        if (this.connection != null && this.preparedStatement != null) {
            return;
        }
        try {
            this.connection = this.dataSource.getConnection();
            this.connection.setAutoCommit(false);
            this.preparedStatement = this.connection.prepareStatement(this.sql);
            if (this.batchArgsField != null && ((DruidPooledPreparedStatement) this.preparedStatement).getStatement() instanceof StatementImpl) {
                this.batchArgsField.set((StatementImpl) ((DruidPooledPreparedStatement) this.preparedStatement).getStatement(), new ArrayList<Object>(stepSize));
                this.tsdf.set(((com.mysql.jdbc.PreparedStatement) ((DruidPooledPreparedStatement) this.preparedStatement).getStatement()),
                        new DateFormatter("''yyyy-MM-dd HH:mm:ss"));
            }
            return;
        } catch (Exception e) {
            LOG.error("Mysql-Handler-Prepare Error :", e);
            this.connection = null;
            this.preparedStatement = null;
            Util.sleep(1);
        }
        prepare(stepSize);
    }

    @Override public void feed(DataEvent de) {
        try {
            for (int i = 0; i < this.schema.size(); i++) {
                this.preparedStatement.setObject(i + 1, de.getTarget()[i], this.schema.get(i).getRight());
            }
            this.preparedStatement.addBatch();
        } catch (Exception e) {
            LOG.error("Mysql-Handler-Feed Error :", e);
            LOG.error("Data :", de.toString());
            Util.sleep(1);
            feed(de);
        }
    }

    @Override public void flush() {
        try {
            this.preparedStatement.executeBatch();
            this.connection.commit();
        } catch (Exception e) {
            LOG.error("Mysql-Handler-Flush Error :", e);
            while (retry()) {
                Util.sleep(1);
            }
        } finally {
            close();
        }
    }

    @Override public boolean retry() {
        try {
            List<Object> batchParams = ((JDBC4PreparedStatement) ((DruidPooledPreparedStatement) this.preparedStatement).getStatement()).getBatchedArgs();
            Connection tc = this.dataSource.getConnection();
            tc.setAutoCommit(false);
            PreparedStatement tp = tc.prepareStatement(this.sql);
            StatementImpl target = (StatementImpl) ((DruidPooledPreparedStatement) tp).getStatement();
            List<Object> newParams = new ArrayList<>(batchParams);
            this.batchArgsField.set(target, newParams);
            tp.executeBatch();
            tc.commit();
            tp.close();
            tc.close();
        } catch (Throwable e) {
            LOG.error("Mysql-Handler-Retry Error :", e);
            return false;
        }
        return true;
    }

    @Override public void close() {
        try {
            if (this.preparedStatement != null)
                this.preparedStatement.close();
            if (this.connection != null)
                this.connection.close();
        } catch (Exception e) {
            LOG.error("Mysql-Handler-Close Error :", e);
        }
        this.connection = null;
        this.preparedStatement = null;
    }
}
