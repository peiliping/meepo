package meepo.transform.sink.batch;

import meepo.transform.channel.DataEvent;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Created by peiliping on 17-5-11.
 */
public interface IHandler {

    public void init();

    public void truncate(String params);

    public void prepare(int stepSize);

    public void feed(DataEvent de, List<Pair<String, Integer>> schema);

    public void flush();

    public boolean retry();

    public void close();

}
