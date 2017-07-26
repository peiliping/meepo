package meepo.transform.sink.redis;

import meepo.transform.config.TaskContext;
import meepo.transform.sink.batch.AbstractBatchSink;
import org.redisson.Redisson;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

/**
 * Created by peiliping on 17-5-18.
 */
public class RedisSink extends AbstractBatchSink {

    private Redisson redis;

    public RedisSink(String name, int index, TaskContext context) {
        super(name, index, context);
        Config conf = new Config();
        SingleServerConfig sc = conf.useSingleServer();
        sc.setAddress(context.get("address"));
        sc.setPassword(context.get("password"));
        sc.setDatabase(context.getInteger("dbnum", 0));
        sc.setClientName(name);
        this.redis = (Redisson) Redisson.create(conf);
        super.handler = new RedisHandler(this.redis, super.schema, context.getString("builder", "COUPLE"));
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        try {
            this.redis.shutdown();
        } catch (Throwable e) {
            LOG.error("Close Redis Error", e);
        }
    }
}
