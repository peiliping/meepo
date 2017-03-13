package meepo.transform.channel.plugin.TypeConvert;

import meepo.transform.config.TaskContext;
import meepo.util.ParquetTypeMapping;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Created by peiliping on 17-3-8.
 */
public class ParquetTypeConvertPlugin extends TypeConvertPlugin {

    public ParquetTypeConvertPlugin(TaskContext context) {
        super(context);
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        sink.addAll(source);
        ParquetTypeMapping.jdbcTypeMatchParquet(sink);
        _autoMatchSchema(source, sink);
    }
}
