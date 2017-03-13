package meepo.transform.source;

import meepo.transform.channel.DataEvent;
import meepo.transform.report.IReportItem;

/**
 * Created by peiliping on 17-3-3.
 */
public interface ISource extends Runnable {

    void start();

    DataEvent feedOne();

    void pushOne();

    void work() throws Exception;

    void stop();

    void end();

    boolean isRunning();

    IReportItem report();

}
