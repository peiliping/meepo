package meepo.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peiliping on 17-3-6.
 */
public class Util {

    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static void sleep(long sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static void sleepMS(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
        }
    }

}
