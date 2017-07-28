package meepo.transform.sink.rdb.handlers;

import meepo.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;

/**
 * Created by peiliping on 17-7-28.
 */
public class VerticaHandler extends MysqlHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(VerticaHandler.class);

    public VerticaHandler(DataSource ds, String sql, List<Pair<String, Integer>> schema) {
        super(ds, sql, schema);
        super.type = "vertica";
    }

    @Override
    public void init() {
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
            LOG.error(super.type + "-Handler-Prepare Error :", e);
            this.connection = null;
            this.preparedStatement = null;
            Util.sleep(1);
        }
        prepare(stepSize);
    }

    @Override
    public boolean retry() {
        return true;
    }
}
