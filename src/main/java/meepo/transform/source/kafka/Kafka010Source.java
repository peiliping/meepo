package meepo.transform.source.kafka;

import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by peiliping on 17-7-27.
 */
public class Kafka010Source extends KafkaSource {

    private KafkaConsumer<String, byte[]> consumer;

    private Iterator<ConsumerRecord<String, byte[]>> consumerIterator;

    public Kafka010Source(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
        this.consumer = new KafkaConsumer<String, byte[]>(toProps());
        this.consumer.subscribe(Arrays.asList(context.get("topic")));
    }

    @Override
    protected Pair<String, byte[]> poll(long remain) {
        if (this.consumerIterator == null || !this.consumerIterator.hasNext()) {
            ConsumerRecords<String, byte[]> records = this.consumer.poll(remain);
            this.consumerIterator = records.iterator();
        }
        if (!this.consumerIterator.hasNext())
            return super.NULL_MSG;
        ConsumerRecord<String, byte[]> message = this.consumerIterator.next();
        return Pair.of(message.key(), message.value());
    }

    @Override
    public Properties toProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, super.kafkaContext.get("bootstrap"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, super.kafkaContext.get("group"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, super.kafkaContext.get("msgSize"));
        return props;
    }

    @Override
    public void end() {
        super.end();
        this.consumer.close();
    }
}
