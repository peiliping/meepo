package meepo.transform.channel.plugin;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Created by peiliping on 17-3-21.
 */
public class GroupPlugin extends AbstractPlugin {

    private List<AbstractPlugin> plugins = Lists.newArrayList();

    public GroupPlugin(TaskContext context) {
        super(context);
        Validate.notNull(context.getString("array"));
        List<String> pluginsArrayStr = Lists.newArrayList(context.getString("array").split("\\s"));
        pluginsArrayStr.forEach(pluginName -> {
            Class<? extends AbstractPlugin> pluginClazz = (PluginType.valueOf(pluginName)).clazz;
            try {
                plugins.add(pluginClazz.getConstructor(TaskContext.class).newInstance(new TaskContext("plugin-" + pluginName, context.getSubProperties(pluginName + "."))));
            } catch (Exception e) {
                LOG.error("Plugin Init Error : ", e);
                Validate.isTrue(false);
            }
        });
    }

    @Override public void convert(DataEvent de) {
        for (AbstractPlugin ap : this.plugins) {
            ap.convert(de);
        }
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        for (AbstractPlugin ap : this.plugins) {
            ap.autoMatchSchema(source, sink);
        }
    }
}
