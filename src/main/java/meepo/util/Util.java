package meepo.util;

/**
 * Created by peiliping on 17-3-6.
 */
public class Util {

    public static void sleep(long sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
        }
    }
}
