package meepo.transform.source.kafka;

import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Iterator;

/**
 * Created by peiliping on 17-7-27.
 */
public class Kafka010Source extends KafkaSource {

    private KafkaConsumer<String, byte[]> consumer;

    private Iterator<ConsumerRecord<String, byte[]>> consumerIterator;

    public Kafka010Source(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
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
}
