package meepo.util;

import com.google.common.collect.Maps;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by peiliping on 17-3-15.
 */
public class DataSourceCache {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceCache.class);

    private final static ConcurrentMap<String, Pair<DataSource, AtomicInteger>> CACHES = Maps.newConcurrentMap();

    public synchronized static DataSource createDataSource(String name, TaskContext context) {
        Pair<DataSource, AtomicInteger> item = CACHES.get(name);
        if (item != null) {
            item.getRight().incrementAndGet();
            return item.getLeft();
        } else {
            LOG.info("Create DataSource : " + name);
            DataSource ds = Util.createDataSource(context);
            CACHES.put(name, Pair.of(ds, new AtomicInteger(1)));
            return ds;
        }
    }

    public synchronized static void close(String name) {
        Pair<DataSource, AtomicInteger> item = CACHES.get(name);
        Validate.notNull(item);
        if (item.getRight().decrementAndGet() == 0) {
            Util.closeDataSource(item.getLeft());
            LOG.info("Close DataSource : " + name);
            CACHES.remove(name);
        }
    }

}
