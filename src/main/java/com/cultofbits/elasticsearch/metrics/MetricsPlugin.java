package com.cultofbits.elasticsearch.metrics;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

public class MetricsPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "metrics-plugin";
    }

    @Override
    public String description() {
        return "Tracks ES stats with dropwizard metrics";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(MetricsService.class);
        return services;
    }
}
