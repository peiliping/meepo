package meepo.transform.channel.plugin;

/**
 * Created by peiliping on 17-3-6.
 */
public enum PluginType {

    DEFAULT(DefaultPlugin.class),

    TYPECONVERT(TypeConvertPlugin.class);
    
    public Class<? extends AbstractPlugin> clazz;

    PluginType(Class<? extends AbstractPlugin> clazz) {
        this.clazz = clazz;
    }
}
