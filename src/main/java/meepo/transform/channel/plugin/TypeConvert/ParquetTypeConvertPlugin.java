package meepo.transform.channel.plugin.TypeConvert;

import meepo.transform.config.TaskContext;
import meepo.util.ParquetTypeMapping;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.function.Consumer;

/**
 * Created by peiliping on 17-3-8.
 */
public class ParquetTypeConvertPlugin extends TypeConvertPlugin {

    public ParquetTypeConvertPlugin(TaskContext context) {
        super(context);
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        sink.addAll(source);
        for (int i = 0; i < sink.size(); i++) {
            Pair<String, Integer> item = sink.get(i);
            if (ParquetTypeMapping.J2J.containsKey(item.getRight())) {
                Integer newType = ParquetTypeMapping.J2J.get(item.getRight());
                Validate.notNull(ParquetTypeMapping.J2P.get(newType));
                sink.set(i, Pair.of(item.getLeft(), newType));
            }
        }
        _autoMatchSchema(source, sink);
    }
}
