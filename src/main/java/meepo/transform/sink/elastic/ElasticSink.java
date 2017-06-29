package meepo.transform.sink.elastic;

import com.google.common.collect.Lists;
import meepo.transform.config.TaskContext;
import meepo.transform.sink.batch.AbstractBatchSink;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created by peiliping on 17-6-28.
 */
public class ElasticSink extends AbstractBatchSink {

    private TransportClient esClient;

    public ElasticSink(String name, int index, TaskContext context) {
        super(name, index, context);
        this.esClient = new PreBuiltTransportClient(Settings.EMPTY);
        List<String> addresses = Lists.newArrayList(context.getString("address").split(","));
        int port = context.getInteger("port", 9300);
        addresses.forEach(ad -> {
            try {
                this.esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ad), port));
            } catch (UnknownHostException e) {
            }
        });
        String indexName = context.getString("esIndex", "default");
        String typeName = context.getString("esType", "default");
        super.handler = new ElasticHandler(this.esClient, indexName, typeName, super.schema);
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        this.esClient.close();
    }
}
