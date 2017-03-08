package meepo.transform.channel.plugin.TypeConvert;

import com.google.common.collect.Maps;
import meepo.transform.channel.DataEvent;
import meepo.transform.channel.plugin.DefaultPlugin;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

/**
 * Created by peiliping on 17-3-8.
 */
public class TypeConvertPlugin extends DefaultPlugin {

    private boolean needConvert = false;

    private Map<Integer, Pair<Integer, Integer>> notMatch = Maps.newHashMap();

    public TypeConvertPlugin(TaskContext context) {
        super(context);
    }

    @Override public void convert(DataEvent de) {
        if (this.needConvert) {

        } else {
            super.convert(de);
        }
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        if (sink.isEmpty()) {
            sink.addAll(source);
            this.needConvert = false;
            return;
        }
        Validate.isTrue(source.size() == sink.size());
        for (int i = 0; i < source.size(); i++) {
            Pair<String, Integer> l = source.get(i);
            Pair<String, Integer> r = sink.get(i);
            if (l.getRight().equals(r.getRight())) {
                continue;
            } else {
                notMatch.put(i, Pair.of(l.getRight(), r.getRight()));
            }
        }
        if (notMatch.size() > 0) {
            this.needConvert = true;
        }
    }
}
