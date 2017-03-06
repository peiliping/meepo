package meepo.transform.source;

import meepo.transform.channel.DataEvent;

/**
 * Created by peiliping on 17-3-3.
 */
public interface ISource extends Runnable {

    void start();

    DataEvent feedOne();

    Object[] eventFactory();

    void work();

    void end();

    boolean isRunning();

}
