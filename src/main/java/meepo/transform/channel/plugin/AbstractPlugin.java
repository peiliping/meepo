package meepo.transform.channel.plugin;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by peiliping on 17-3-8.
 */
public abstract class AbstractPlugin {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractPlugin.class);

    public AbstractPlugin(TaskContext context) {
    }

    public abstract void convert(DataEvent de);

    public abstract void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink);

    public abstract void close();

}
