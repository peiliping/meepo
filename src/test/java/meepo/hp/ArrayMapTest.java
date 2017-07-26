package meepo.hp;

import meepo.util.hp.map.ArrayMap;
import meepo.util.hp.map.Keys;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by peiliping on 17-6-30.
 */
public class ArrayMapTest {

    @Test
    public void arrayMapTest() throws InterruptedException {
        Thread.sleep(3000);
        long st = System.currentTimeMillis();

        Pair<Integer, String> p0 = Pair.of(0, "topic");
        Pair<Integer, String> p1 = Pair.of(1, "channel");
        Pair<Integer, String> p2 = Pair.of(2, "timestamps");
        Pair<Integer, String> p3 = Pair.of(3, "key");
        Pair<Integer, String> p4 = Pair.of(4, "type");
        Pair<Integer, String> p5 = Pair.of(5, "path");
        Pair<Integer, String> p6 = Pair.of(6, "props");
        Keys<String> keys = new Keys<>(p0, p1, p2, p3, p4, p5, p6);

        for (int i = 0; i < 100000000; i++) {
            ArrayMap<String, String> map = new ArrayMap<>(keys);
            map.putP(p0, "abcde");
            map.putP(p1, "bcdef");
            map.putP(p2, System.currentTimeMillis() + "");
            map.putP(p3, "cdefg");
            map.putP(p4, "defgh");
            map.putP(p5, "efghi");
            map.putP(p6, "fghij");
            map.getP(p0);
            map.getP(p1);
            map.getP(p2);
            map.getP(p3);
            map.getP(p4);
            map.getP(p5);
            map.getP(p6);
            map.entrySet();
        }
        long et = System.currentTimeMillis();
        System.out.println("ArrayMap : " + (et - st));
    }

    @Test
    public void hashMapTest() throws InterruptedException {
        Thread.sleep(3000);
        long st = System.currentTimeMillis();

        for (int i = 0; i < 100000000; i++) {
            Map<String, String> map = new HashMap<>(16);
            map.put("topic", "abcde");
            map.put("channel", "bcdef");
            map.put("timestamps", System.currentTimeMillis() + "");
            map.put("key", "cdefg");
            map.put("type", "defgh");
            map.put("path", "efghi");
            map.put("props", "fghij");
            map.get("topic");
            map.get("channel");
            map.get("timestamps");
            map.get("key");
            map.get("type");
            map.get("path");
            map.get("props");
            map.entrySet();
        }
        long et = System.currentTimeMillis();
        System.out.println("HashMap : " + (et - st));
    }
}
