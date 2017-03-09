package meepo.transform.source;

import meepo.transform.channel.DataEvent;

/**
 * Created by peiliping on 17-3-3.
 */
public interface ISource extends Runnable {

    void start();

    DataEvent feedOne();

    void eventFactory(DataEvent de);

    void work();

    void stop();

    void end();

    boolean isRunning();

}
