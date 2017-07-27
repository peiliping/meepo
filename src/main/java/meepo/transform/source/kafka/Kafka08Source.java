package meepo.transform.source.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import meepo.transform.channel.RingbufferChannel;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by peiliping on 17-7-27.
 */
public class Kafka08Source extends KafkaSource {

    private ConsumerConnector consumer;

    private ConsumerIterator<String, byte[]> consumerIterator;

    public Kafka08Source(String name, int index, int totalNum, TaskContext context, RingbufferChannel rb) {
        super(name, index, totalNum, context, rb);
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
}
