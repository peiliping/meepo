package meepo.transform.sink.redis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import meepo.transform.channel.DataEvent;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RBatch;
import org.redisson.client.codec.MapScanCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.codec.JsonJacksonCodec;

import java.util.List;
import java.util.Map;

/**
 * Created by peiliping on 17-5-18.
 */
public interface IBuilder {

    public void build(RBatch batch, DataEvent event, List<Pair<String, Integer>> schema);

    /*==================================================*/

    public static IBuilder COUPLE = (batch, event, schema) -> batch.getBucket(String.valueOf(event.getTarget()[0])).setAsync(event.getTarget()[1]);

    public static IBuilder ARRAY = (batch, event, schema) -> batch.getList(String.valueOf(event.getTarget()[0])).addAllAsync(0, Lists.newArrayList(event.getTarget()));

    public static IBuilder MAP = (batch, event, schema) -> {
        Map<String, Object> item = Maps.newHashMapWithExpectedSize(schema.size());
        for (int i = 0; i < schema.size(); i++)
            item.put(schema.get(i).getLeft(), event.getTarget()[i]);
        batch.getMap(String.valueOf(event.getTarget()[0]), new MapScanCodec(StringCodec.INSTANCE, JsonJacksonCodec.INSTANCE)).putAllAsync(item);
    };

}
