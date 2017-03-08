package meepo.transform.channel.plugin;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;

/**
 * Created by peiliping on 17-3-8.
 */
public abstract class AbstractPlugin {

    protected TaskContext context;

    public AbstractPlugin(TaskContext context) {
        this.context = context;
    }

    public abstract void convert(DataEvent de);

}
