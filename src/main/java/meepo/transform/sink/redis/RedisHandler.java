package meepo.transform.sink.redis;

import com.google.common.collect.Maps;
import meepo.transform.channel.DataEvent;
import meepo.transform.sink.batch.IHandler;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by peiliping on 17-5-25.
 */
public class RedisHandler implements IHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(RedisHandler.class);

    private static Map<String, IBuilder> CONST = Maps.newHashMap();

    static {
        CONST.put("COUPLE", IBuilder.COUPLE);
        CONST.put("ARRAY", IBuilder.ARRAY);
        CONST.put("MAP", IBuilder.MAP);
    }

    private List<Pair<String, Integer>> schema;

    private Redisson redis;

    private RBatch batch;

    private IBuilder builder;

    public RedisHandler(Redisson redis, List<Pair<String, Integer>> schema, String builderName) {
        this.redis = redis;
        this.builder = CONST.get(builderName);
        this.schema = schema;
    }

    @Override
    public void init() {
    }

    @Override
    public void truncate(String params) {
    }

    @Override
    public void prepare(int stepSize) {
        if (this.batch == null) {
            this.batch = this.redis.createBatch();
            this.batch.retryAttempts(3);
            this.batch.retryInterval(3, TimeUnit.SECONDS);
            this.batch.timeout(5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void feed(DataEvent de) {
        this.builder.build(this.batch, de, this.schema);
    }

    @Override
    public void flush() {
        try {
            this.batch.execute();
        } catch (Exception e) {
            LOG.error("Exec Redis Batch Error ", e);
        } finally {
            close();
        }
    }

    @Override
    public boolean retry() {
        return false;
    }

    @Override
    public void close() {
        if (this.batch != null) {
            this.batch = null;
        }
    }
}
