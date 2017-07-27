package meepo.transform.source.kafka.serialize;

import meepo.transform.channel.DataEvent;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Created by peiliping on 17-7-27.
 */
public interface IKafkaSerialize {

    public void initSchema(List<Pair<String, Integer>> schema);

    public void parse(byte[] msg, DataEvent de);

}
