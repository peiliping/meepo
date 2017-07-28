package meepo.transform.sink.rdb.handlers;

import meepo.transform.channel.DataEvent;
import meepo.transform.sink.batch.IHandler;
import meepo.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

/**
 * Created by peiliping on 17-7-28.
 */
public class VerticaHandler implements IHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(VerticaHandler.class);

    protected DataSource dataSource;

    protected String sql;

    protected Connection connection;

    protected PreparedStatement preparedStatement;

    protected List<Pair<String, Integer>> schema;

    public VerticaHandler(DataSource ds, String sql, List<Pair<String, Integer>> schema) {
        this.dataSource = ds;
        this.sql = sql;
        this.schema = schema;
    }

    @Override
    public void init() {
    }

    @Override
    public void truncate(String tableName) {
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

    @Override
    public void prepare(int stepSize) {
        if (this.connection != null && this.preparedStatement != null) {
            return;
        }
        try {
            this.connection = this.dataSource.getConnection();
            this.connection.setAutoCommit(false);
            this.preparedStatement = this.connection.prepareStatement(this.sql);
            return;
        } catch (Exception e) {
            LOG.error("Vertica-Handler-Prepare Error :", e);
            this.connection = null;
            this.preparedStatement = null;
            Util.sleep(1);
        }
        prepare(stepSize);
    }

    @Override
    public void feed(DataEvent de) {
        try {
            for (int i = 0; i < this.schema.size(); i++) {
                this.preparedStatement.setObject(i + 1, de.getTarget()[i], this.schema.get(i).getRight());
            }
            this.preparedStatement.addBatch();
        } catch (Exception e) {
            LOG.error("Vertica-Handler-Feed Error :", e);
            LOG.error("Data :", de.toString());
            Util.sleep(1);
            feed(de);
        }
    }

    @Override
    public void flush() {
        try {
            this.preparedStatement.executeBatch();
            this.connection.commit();
        } catch (Exception e) {
            LOG.error("Vertica-Handler-Flush Error :", e);
            int i = 0;
            while (!retry() && i++ < 1024) {
                Util.sleep(1);
            }
        } finally {
            close();
        }
    }

    @Override
    public boolean retry() {
        return true;
    }

    @Override
    public void close() {
        try {
            if (this.preparedStatement != null)
                this.preparedStatement.close();
            if (this.connection != null)
                this.connection.close();
        } catch (Exception e) {
            LOG.error("Vertica-Handler-Close Error :", e);
        }
        this.connection = null;
        this.preparedStatement = null;
    }
}
