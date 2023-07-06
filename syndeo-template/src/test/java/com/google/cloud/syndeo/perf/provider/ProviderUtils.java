package com.google.cloud.syndeo.perf.provider;

import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.Row;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProviderUtils {
    private ProviderUtils() {
    }

    public static <ConfigT> Map<String, Object> configToMap(ConfigT config) {
        try {
            Row rowConfig =
                    SchemaRegistry.createDefault()
                            .getToRowFunction((Class<ConfigT>) config.getClass())
                            .apply(config);
            return rowConfig.getSchema().getFields().stream()
                    .filter(f -> rowConfig.getValue(f.getName()) != null)
                    .map(f -> Map.entry(f.getName(), rowConfig.getValue(f.getName())))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (NoSuchSchemaException e) {
            throw new RuntimeException(e);
        }
    }

    private static Provider instantiateProvider(Class<? extends Provider> clazz, ProviderContext ctx) {
        for (Constructor<?> constructor : clazz.getConstructors()) {
            if (constructor.getParameterCount() != 1 || constructor.getParameterTypes()[0] != ProviderContext.class) {
                continue;
            }
            try {
                return (Provider) constructor.newInstance(ctx);
            } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalArgumentException("Cannot instantiate " + clazz.getName() + " as a Provider, because it does not have a constructor accepting a single ProviderContext argument");
    }

    public static Source instantiateSource(Class<? extends Source> clazz, ProviderContext ctx) {
        return (Source) instantiateProvider(clazz, ctx);
    }

    public static Sink instantiateSink(Class<? extends Sink> clazz, ProviderContext ctx) {
        return (Sink) instantiateProvider(clazz, ctx);
    }
}
