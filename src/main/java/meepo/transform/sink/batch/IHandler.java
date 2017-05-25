package meepo.transform.sink.batch;

import meepo.transform.channel.DataEvent;

/**
 * Created by peiliping on 17-5-11.
 */
public interface IHandler {

    public void init();

    public void truncate(String params);

    public void prepare(int stepSize);

    public void feed(DataEvent de);

    public void flush();

    public boolean retry();

    public void close();

}
