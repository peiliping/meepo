package meepo.transform.source.kafka.serialize;

import meepo.transform.channel.DataEvent;

/**
 * Created by peiliping on 17-7-27.
 */
public interface IKafkaSerialize {

    public void parse(byte[] msg, DataEvent de);

}
