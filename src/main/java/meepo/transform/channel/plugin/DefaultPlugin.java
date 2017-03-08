package meepo.transform.channel.plugin;

import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;

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
}
