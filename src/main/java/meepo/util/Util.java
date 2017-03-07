package meepo.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * Created by peiliping on 17-3-6.
 */
public class Util {

    protected static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static void sleep(long sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static DataSource createDataSource(TaskContext context) {
        try {
            return DruidDataSourceFactory.createDataSource(context.getParameters());
        } catch (Exception e) {
            LOG.error("Create DataSource Error : ", context.toString());
            Validate.isTrue(false);
        }
        return null;
    }
}
