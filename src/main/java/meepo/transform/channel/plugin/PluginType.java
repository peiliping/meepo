package meepo.transform.channel.plugin;

import meepo.transform.channel.plugin.replace.ComplexReplacePlugin;
import meepo.transform.channel.plugin.replace.ReplacePlugin;
import meepo.transform.channel.plugin.typeconvert.ParquetTypeConvertPlugin;
import meepo.transform.channel.plugin.typeconvert.TypeConvertPlugin;

/**
 * Created by peiliping on 17-3-6.
 */
public enum PluginType {

    DEFAULT(DefaultPlugin.class),

    TYPECONVERT(TypeConvertPlugin.class),

    PARQUETTYPECONVERT(ParquetTypeConvertPlugin.class),

    REPLACEPLUGIN(ReplacePlugin.class),

    COMPLEXREPLACEPLUGIN(ComplexReplacePlugin.class),

    GROUPPLUGIN(GroupPlugin.class);

    public Class<? extends AbstractPlugin> clazz;

    PluginType(Class<? extends AbstractPlugin> clazz) {
        this.clazz = clazz;
    }
}
