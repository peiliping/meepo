package meepo.transform.source.kafka.serialize;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Created by peiliping on 17-7-27.
 */
public abstract class IKafkaSerialize {

    protected TaskContext context;

    public IKafkaSerialize(TaskContext ct) {
        this.context = ct;
    }

    public abstract void initSchema(List<Pair<String, Integer>> schema);

    public abstract void parse(byte[] msg, DataEvent de);

}
