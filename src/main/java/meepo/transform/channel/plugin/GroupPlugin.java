package meepo.transform.channel.plugin;

import com.google.common.collect.Lists;
import meepo.transform.channel.DataEvent;
import meepo.transform.config.TaskContext;
import meepo.transform.report.IReportItem;
import meepo.transform.report.MultiReport;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Created by peiliping on 17-3-21.
 */
public class GroupPlugin extends AbstractPlugin {

    private List<AbstractPlugin> plugins = Lists.newArrayList();

    private int theEndPluginPosition;

    public GroupPlugin(TaskContext context) {
        super(context);
        Validate.notNull(context.getString("array"));
        List<String> pluginsArrayStr = Lists.newArrayList(context.getString("array").split("\\s"));
        pluginsArrayStr.forEach(pluginName -> {
            String ptype = pluginName.split("-")[0];
            String pname = pluginName.split("-")[1];
            Class<? extends AbstractPlugin> pluginClazz = (PluginType.valueOf(ptype)).clazz;
            try {
                plugins.add(pluginClazz.getConstructor(TaskContext.class).newInstance(new TaskContext("plugin-" + pluginName, context.getSubProperties(pname + "."))));
            } catch (Exception e) {
                LOG.error("Plugin Init Error : ", e);
                Validate.isTrue(false);
            }
        });
        this.theEndPluginPosition = this.plugins.size() - 1;
    }

    @Override public void convert(DataEvent de, boolean theEnd) {
        for (int i = 0; i < this.plugins.size(); i++) {
            this.plugins.get(i).convert(de, theEndPluginPosition == i);
        }
    }

    @Override public void autoMatchSchema(List<Pair<String, Integer>> source, List<Pair<String, Integer>> sink) {
        for (AbstractPlugin ap : this.plugins) {
            ap.autoMatchSchema(source, sink);
        }
    }

    @Override public void close() {
        this.plugins.forEach(abstractPlugin -> abstractPlugin.close());
    }

    @Override public IReportItem report() {
        MultiReport mr = new MultiReport();
        this.plugins.forEach(ap -> mr.getReports().add(ap.report()));
        return mr;
    }
}
