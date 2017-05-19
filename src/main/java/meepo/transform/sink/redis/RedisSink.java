package meepo.transform.sink.redis;

import com.google.common.collect.Maps;
import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.AbstractSink;
import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by peiliping on 17-5-18.
 */
public class RedisSink extends AbstractSink {

    private Redisson redis;

    private RBatch batch;

    private int stepSize;

    private long lastCommit;

    private long lastFlushTS;

    private IBuilder builder;

    public static Map<String, IBuilder> CONST = Maps.newHashMap();

    static {
        CONST.put("COUPLE", IBuilder.COUPLE);
        CONST.put("ARRAY", IBuilder.ARRAY);
        CONST.put("MAP", IBuilder.MAP);
    }

    public RedisSink(String name, int index, TaskContext context) {
        super(name, index, context);
        Config conf = new Config();
        SingleServerConfig sc = conf.useSingleServer();
        sc.setAddress(context.get("address"));
        sc.setPassword(context.get("password"));
        sc.setClientName(name);
        this.redis = (Redisson) Redisson.create(conf);
        this.lastFlushTS = System.currentTimeMillis();
        this.stepSize = context.getInteger("stepSize", 100);
        this.builder = CONST.get(context.getString("builder", "COUPLE"));
    }

    @Override public void onStart() {
        super.onStart();
        checkBatch();
    }

    @Override public void onEvent(Object event) throws Exception {
        super.RUNNING = true;
        checkBatch();
        super.count++;
        DataEvent de = (DataEvent) event;
        this.builder.build(this.batch, de, super.schema);
        if (super.count - this.lastCommit >= this.stepSize || System.currentTimeMillis() - this.lastFlushTS > 3000) {
            execBatch();
        }
    }

    @Override public void timeOut() {
        execBatch();
    }

    @Override public void onShutdown() {
        execBatch();
        try {
            this.redis.shutdown();
        } catch (Throwable e) {
            LOG.error("Close Redis Error", e);
        }
        super.onShutdown();
    }

    private void checkBatch() {
        if (this.batch == null) {
            this.batch = this.redis.createBatch();
            this.batch.retryAttempts(3);
            this.batch.retryInterval(3, TimeUnit.SECONDS);
            this.batch.timeout(5, TimeUnit.SECONDS);
        }
    }

    private void execBatch() {
        if (this.batch != null && super.count - this.lastCommit > 0) {
            this.batch.execute();
            super.RUNNING = false;
            this.lastCommit = super.count;
        }
        this.batch = null;
        this.lastFlushTS = System.currentTimeMillis();
    }
}
