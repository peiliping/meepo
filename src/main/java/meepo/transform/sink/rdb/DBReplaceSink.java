package meepo.transform.sink.rdb;

import meepo.transform.config.TaskContext;

/**
 * Created by peiliping on 17-3-9.
 */
public class DBReplaceSink extends DBSink {

    public DBReplaceSink(String name, int index, TaskContext context) {
        super(name, index, context);
    }

    @Override public String buildSQL() {
        return "REPLACE INTO " + super.tableName + " (" + super.columnNames + ") VALUES ( " + super.paramsStr + ")";
    }
}
