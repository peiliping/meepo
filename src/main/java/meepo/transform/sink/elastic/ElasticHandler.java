package meepo.transform.sink.elastic;

import com.google.common.collect.Maps;
import meepo.transform.channel.DataEvent;
import meepo.transform.sink.batch.IHandler;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by peiliping on 17-6-28.
 */
public class ElasticHandler implements IHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(ElasticHandler.class);

    private TransportClient client;

    private String index;

    private String type;

    private List<Pair<String, Integer>> schema;

    private BulkRequestBuilder bulkRequest;

    private Map<String, Object> item = Maps.newHashMap();

    public ElasticHandler(TransportClient client, String index, String type, List<Pair<String, Integer>> schema) {
        this.client = client;
        this.index = index;
        this.type = type;
        this.schema = schema;
    }

    @Override
    public void init() {
        this.schema.forEach(pair -> item.put(pair.getKey(), null));
    }

    @Override
    public void truncate(String params) {
    }

    @Override
    public void prepare(int stepSize) {
        if (this.bulkRequest == null)
            this.bulkRequest = this.client.prepareBulk();
    }

    @Override
    public void feed(DataEvent de) {
        for (int i = 0; i < this.schema.size(); i++) {
            this.item.put(this.schema.get(i).getKey(), de.getTarget()[i]);
        }
        this.bulkRequest.add(this.client.prepareIndex(this.index, this.type).setSource(this.item));
    }

    @Override
    public void flush() {
        if (this.bulkRequest != null)
            this.bulkRequest.execute().actionGet();
        this.bulkRequest = null;
    }

    @Override
    public boolean retry() {
        return true;
    }

    @Override
    public void close() {
        if (this.client != null)
            this.client.close();
    }
}
