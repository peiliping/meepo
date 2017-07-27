package meepo.transform.source.kafka;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by peiliping on 17-7-27.
 */
public class Kafka08Source extends KafkaSource {

    private ConsumerConnector consumer;

    private ConsumerIterator<String, byte[]> consumerIterator;

    public Kafka08Source(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        ConsumerConfig consumerConfig = new ConsumerConfig(toProps());
        this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(context.get("topic"), 1);
        Map<String, List<KafkaStream<String, byte[]>>> consumerMap =
                consumer.createMessageStreams(topicCountMap, new StringDecoder(new VerifiableProperties()), new DefaultDecoder(new VerifiableProperties()));
        List<KafkaStream<String, byte[]>> topicList = consumerMap.get(context.get("topic"));
        KafkaStream<String, byte[]> stream = topicList.get(0);
        this.consumerIterator = stream.iterator();
    }

    @Override
    protected Pair<String, byte[]> poll(long remain) {
        if (hasNext()) {
            MessageAndMetadata<String, byte[]> messageAndMetadata = this.consumerIterator.next();
            return Pair.of(messageAndMetadata.key(), messageAndMetadata.message());
        }
        return super.NULL_MSG;
    }

    private boolean hasNext() {
        try {
            return this.consumerIterator.hasNext();
        } catch (ConsumerTimeoutException e) {
            return false;
        }
    }

    @Override
    public Properties toProps() {
        Properties props = new Properties();
        props.put("zookeeper.connect", super.kafkaContext.get("zookeeper"));
        props.put("group.id", super.kafkaContext.get("group"));
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", super.kafkaContext.getString("commitInterval", "10000"));
        props.put("consumer.timeout.ms", "3000");
        props.put("fetch.message.max.bytes", super.kafkaContext.get("msgSize"));
        return props;
    }

    @Override
    public void end() {
        super.end();
        this.consumer.shutdown();
    }
}
