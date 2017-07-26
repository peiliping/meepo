package meepo.transform.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.Getter;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by peiliping on 17-3-6.
 */
public class TaskContext {

    @Getter
    private String taskName;

    private Map<String, String> parameters;

    public TaskContext(String taskName) {
        this.taskName = taskName;
        this.parameters = Collections.synchronizedMap(new HashMap<String, String>());
    }

    public TaskContext(String taskName, Map<String, String> paramters) {
        this(taskName);
        this.putAll(paramters);
    }

    public TaskContext(String taskName, Properties ps) {
        this(taskName);
        Enumeration<?> propertyNames = ps.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = (String) propertyNames.nextElement();
            String value = ps.getProperty(name);
            this.parameters.put(name, value);
        }
    }

    public TaskContext(String taskName, String propertyFilePath) throws IOException {
        this(taskName, loadFile(propertyFilePath));
    }

    private static Properties loadFile(String configPath) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(configPath));
        Properties properties = new Properties();
        properties.load(in);
        in.close();
        return properties;
    }

    public ImmutableMap<String, String> getParameters() {
        synchronized (parameters) {
            return ImmutableMap.copyOf(parameters);
        }
    }

    public void clear() {
        parameters.clear();
    }

    public ImmutableMap<String, String> getSubProperties(String prefix) {
        Preconditions.checkArgument(prefix.endsWith("."), "The given prefix does not end with a period (" + prefix + ")");
        Map<String, String> result = Maps.newHashMap();
        synchronized (parameters) {
            for (String key : parameters.keySet()) {
                if (key.startsWith(prefix)) {
                    String name = key.substring(prefix.length());
                    result.put(name, parameters.get(key));
                }
            }
        }
        return ImmutableMap.copyOf(result);
    }

    public void putAll(Map<String, String> map) {
        parameters.putAll(map);
    }

    public void put(String key, String value) {
        parameters.put(key, value);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        String value = get(key);
        if (value != null) {
            return Boolean.parseBoolean(value.trim());
        }
        return defaultValue;
    }

    public Boolean getBoolean(String key) {
        return getBoolean(key, null);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        String value = get(key);
        if (value != null) {
            return Integer.parseInt(value.trim());
        }
        return defaultValue;
    }

    public Integer getInteger(String key) {
        return getInteger(key, null);
    }

    public Long getLong(String key, Long defaultValue) {
        String value = get(key);
        if (value != null) {
            return Long.parseLong(value.trim());
        }
        return defaultValue;
    }

    public Long getLong(String key) {
        return getLong(key, null);
    }

    public Double getDouble(String key, Double defaultValue) {
        String value = get(key);
        if (value != null) {
            return Double.parseDouble(value);
        }
        return defaultValue;
    }

    public Double getDouble(String key) {
        return getDouble(key, null);
    }

    public String getString(String key, String defaultValue) {
        return get(key, defaultValue);
    }

    public String getString(String key) {
        return get(key);
    }

    private String get(String key, String defaultValue) {
        String result = parameters.get(key);
        if (result != null) {
            return result;
        }
        return defaultValue;
    }

    public String get(String key) {
        return get(key, null);
    }

    @Override
    public String toString() {
        return "{ parameters:" + parameters + " }";
    }

}
