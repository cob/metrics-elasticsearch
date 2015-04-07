package com.cultofbits.elasticsearch.metrics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.service.NodeService;

public class MetricsService extends AbstractLifecycleComponent<MetricsService> {

    private final Long interval;

    private Thread thread;
    private MetricsWorker worker;


    @Inject
    public MetricsService(Settings settings,
                          NodeService nodeService, IndicesService indicesService, ClusterService clusterService) {
        super(settings);

        interval = settings.getAsLong("metrics.interval", 60000L);

        String indicesToInclude = settings.get("metrics.indexes-to-include", "");
        worker = new MetricsWorker(logger,
                                   nodeService, indicesService, interval,
                                   clusterService,
                                   indicesToInclude.length() > 0 ? indicesToInclude.split(",") : new String[0]);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (interval <= 0){
            logger.info("metrics.interval <= 0, skipping Metrics Service start.");
            return;
        }

        logger.info("Starting Metrics Service");

        if (thread == null || !thread.isAlive()) {

            thread = new Thread(worker, EsExecutors.threadName(settings, "metrics.exporters"));
            thread.setDaemon(true);
            thread.start();
        }

    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("Stopping Metrics Service");

        if (thread != null && thread.isAlive()) {
            worker.stopping = true;
            thread.interrupt();

            try {
                thread.join(30000L);

            } catch (InterruptedException e) {
                // nothing to do
            }
        }

    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
