package meepo.transform.channel.plugin;

import meepo.transform.channel.plugin.TypeConvert.ParquetTypeConvertPlugin;
import meepo.transform.channel.plugin.TypeConvert.TypeConvertPlugin;

/**
 * Created by peiliping on 17-3-6.
 */
public enum PluginType {

    DEFAULT(DefaultPlugin.class),

    TYPECONVERT(TypeConvertPlugin.class),

    PARQUETTYPECONVERT(ParquetTypeConvertPlugin.class);

    public Class<? extends AbstractPlugin> clazz;

    PluginType(Class<? extends AbstractPlugin> clazz) {
        this.clazz = clazz;
    }
}
