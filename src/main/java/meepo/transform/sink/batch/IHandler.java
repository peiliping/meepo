package meepo.transform.sink.batch;

import meepo.transform.channel.DataEvent;

/**
 * Created by peiliping on 17-5-11.
 */
public interface IHandler {

    void init();

    void truncate(String params);

    void prepare(int stepSize);

    void feed(DataEvent de);

    void flush();

    boolean retry();

    void close();

}
