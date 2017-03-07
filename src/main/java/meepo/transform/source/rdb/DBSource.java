package meepo.transform.source.rdb;

import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import meepo.transform.source.AbstractSource;
import meepo.util.Constants;
import meepo.util.Util;

import javax.sql.DataSource;

/**
 * Created by peiliping on 17-3-7.
 */
public abstract class DBSource extends AbstractSource {

    protected DataSource dataSource;

    public DBSource(String name, int index, TaskContext context, RingbufferChannel rb) {
        super(name, index, context, rb);
        this.dataSource = Util.createDataSource(new TaskContext(Constants.DATASOURCE, context.getSubProperties(Constants.DATASOURCE_)));
    }
}
