package meepo.transform.channel.plugin;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Created by peiliping on 17-3-8.
 */
public class DefaultPlugin extends AbstractPlugin {

    public DefaultPlugin(TaskContext context) {
        super(context);
    }

    @Override public void convert(DataEvent de) {
        de.setTarget(de.getSource());
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
    }

    @Override public void close() {
    }
}
