package meepo.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;

/**
 * Created by peiliping on 17-3-6.
 */
public class Util {

    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static void sleep(long sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static void sleepMS(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
        }
    }

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
}
