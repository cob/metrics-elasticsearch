package com.cultofbits.elasticsearch.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.service.NodeService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsWorker implements Runnable {

    private ESLogger logger;
    private NodeService nodeService;
    private IndicesService indicesService;
    private long interval;

    private ClusterService clusterService;
    private String[] indexesToResolve;
    private String[] indexesToInclude = new String[0];

    private boolean alreadyRegistered = false;
    private boolean indicesResolved = false;

    public volatile boolean stopping;

    private MetricRegistry metrics;
    private Map<String, Long> cachedGauges = new ConcurrentHashMap<>();
    private final Timer timer;

    public MetricsWorker(ESLogger logger,
                         NodeService nodeService, IndicesService indicesService, long interval,
                         ClusterService clusterService, String[] indexesToResolve) {
        this.logger = logger;
        this.nodeService = nodeService;
        this.indicesService = indicesService;
        this.interval = interval;
        this.clusterService = clusterService;
        this.indexesToResolve = indexesToResolve;

        metrics = SharedMetricRegistries.getOrCreate("elasticsearch");
        timer = metrics.timer("metrics.metrics._all.time");
    }


    @Override
    public void run() {
        try {
            while (!stopping) {
                try { Thread.sleep(interval); } catch (InterruptedException e) { /* nothing to do */ }

                Timer.Context time = timer.time();

                try {
                    if (!indicesResolved) resolveIndices();

                    updateTotalStats();

                    updateHealthStats();

                    for (String indexName : indexesToInclude) {
                        IndexService service = indicesService.indexService(indexName);

                        updateIndicesDocsStats(indexName, service);
                        updateIndicesIndexingStats(indexName, service);
                        updateIndicesMergeStats(indexName, service);
                    }

                    if (indicesResolved && !alreadyRegistered) registerMetrics();

                } finally {
                    time.stop();
                }

            }
        } catch (Exception e) {
            logger.error("no more metrics :(", e);
        }

    }

    private void updateHealthStats() {
        ClusterState state = clusterService.state();
        ClusterHealthResponse healthResponse =
            new ClusterHealthResponse("unused cluster name", state.metaData().concreteAllOpenIndices(), state);

        cachedGauges.put("indices.health._all.active-shards", (long) healthResponse.getActiveShards());
        cachedGauges.put("indices.health._all.active-primary-shards", (long) healthResponse.getActivePrimaryShards());
        cachedGauges.put("indices.health._all.relocating-shards", (long) healthResponse.getRelocatingShards());
        cachedGauges.put("indices.health._all.initializing-shards", (long) healthResponse.getInitializingShards());
        cachedGauges.put("indices.health._all.unassigned-shards", (long) healthResponse.getUnassignedShards());


        Map<String, ClusterIndexHealth> indexHealthMap = healthResponse.getIndices();
        for (String indexName : indexesToInclude) {
            if (indexHealthMap.containsKey(indexName)) {
                ClusterIndexHealth health = indexHealthMap.get(indexName);
                cachedGauges.put("indices.health." + indexName + ".active-shards", (long) health.getActiveShards());
                cachedGauges.put("indices.health." + indexName + ".active-primary-shards", (long) health.getActivePrimaryShards());
                cachedGauges.put("indices.health." + indexName + ".relocating-shards", (long) health.getRelocatingShards());
                cachedGauges.put("indices.health." + indexName + ".initializing-shards", (long) health.getInitializingShards());
                cachedGauges.put("indices.health." + indexName + ".unassigned-shards", (long) health.getUnassignedShards());
            }
        }
    }

    private void resolveIndices() {
        if (indexesToResolve.length == 0) {
            logger.info("Will not track indices stats");
            indicesResolved = true;
            return;
        }

        String[] indexesToInclude = clusterService.state().metaData().concreteIndices(
            IndicesOptions.lenientExpandOpen(),
            indexesToResolve
        );

        if (indexesToInclude.length > 0) {
            logger.info("Will track stats for the indices [{}], resolved from [{}]",
                        indexesToInclude,
                        indexesToResolve);
            indicesResolved = true;
            this.indexesToInclude = indexesToInclude;
        }


    }

    void updateTotalStats() {
        NodeStats stats = nodeService.stats();

        NodeIndicesStats indices = stats.getIndices();

        DocsStats docs = indices.getDocs();
        cachedGauges.put("indices.docs._all.count", docs.getCount());
        cachedGauges.put("indices.docs._all.deleted", docs.getDeleted());

        SearchStats.Stats search = indices.getSearch().getTotal();
        cachedGauges.put("indices.search._all.query-time", search.getQueryTimeInMillis());
        cachedGauges.put("indices.search._all.query-count", search.getQueryCount());
        cachedGauges.put("indices.search._all.query-time-per",
                         ratio(search.getQueryTimeInMillis(), search.getQueryCount()));
        cachedGauges.put("indices.search._all.query-current", search.getQueryCurrent());
        cachedGauges.put("indices.search._all.fetch-time", search.getFetchTimeInMillis());
        cachedGauges.put("indices.search._all.fetch-count", search.getFetchCount());
        cachedGauges.put("indices.search._all.fetch-time-per",
                         ratio(search.getFetchTimeInMillis(), search.getFetchCount()));
        cachedGauges.put("indices.search._all.fetch-current", search.getFetchCurrent());

        IndexingStats.Stats indexing = indices.getIndexing().getTotal();
        cachedGauges.put("indices.indexing._all.index-time", indexing.getIndexTimeInMillis());
        cachedGauges.put("indices.indexing._all.index-count", indexing.getIndexCount());
        cachedGauges.put("indices.indexing._all.index-time-per",
                         ratio(indexing.getIndexTimeInMillis(), indexing.getIndexCount()));
        cachedGauges.put("indices.indexing._all.index-current", indexing.getIndexCurrent());
        cachedGauges.put("indices.indexing._all.delete-time", indexing.getDeleteTimeInMillis());
        cachedGauges.put("indices.indexing._all.delete-count", indexing.getDeleteCount());
        cachedGauges.put("indices.indexing._all.delete-time-per",
                         ratio(indexing.getDeleteTimeInMillis(), indexing.getDeleteCount()));
        cachedGauges.put("indices.indexing._all.delete-current", indexing.getDeleteCurrent());


        FlushStats flush = indices.getFlush();
        cachedGauges.put("indices.flush._all.time", flush.getTotalTimeInMillis());
        cachedGauges.put("indices.flush._all.count", flush.getTotal());
        cachedGauges.put("indices.flush._all.time-per",
                         ratio(flush.getTotalTimeInMillis(), flush.getTotal()));

        MergeStats merge = indices.getMerge();
        cachedGauges.put("indices.merge._all.time", merge.getTotalTimeInMillis());
        cachedGauges.put("indices.merge._all.count", merge.getTotal());
        cachedGauges.put("indices.merge._all.time-per",
                         ratio(merge.getTotalTimeInMillis(), merge.getTotal()));
        cachedGauges.put("indices.merge._all.docs", merge.getTotalNumDocs());
        cachedGauges.put("indices.merge._all.size", merge.getTotalSizeInBytes());
        cachedGauges.put("indices.merge._all.current", merge.getCurrent());

        RefreshStats refresh = indices.getRefresh();
        cachedGauges.put("indices.refresh._all.time", refresh.getTotalTimeInMillis());
        cachedGauges.put("indices.refresh._all.count", refresh.getTotal());
        cachedGauges.put("indices.refresh._all.time-per",
                         ratio(refresh.getTotalTimeInMillis(), refresh.getTotal()));

        SegmentsStats segments = indices.getSegments();
        cachedGauges.put("indices.segments._all.count", segments.getCount());
        cachedGauges.put("indices.segments._all.memory", segments.getMemoryInBytes());
        cachedGauges.put("indices.segments._all.index-writer-memory", segments.getIndexWriterMemoryInBytes());

    }

    private void updateIndicesDocsStats(String indexName, IndexService service) {
        long count = 0;
        long deleted = 0;

        try {
            if (service != null) {
                for (IndexShard shard : service) {
                    DocsStats stats = shard.docStats();
                    count += stats.getCount();
                    deleted += stats.getDeleted();
                }
            }
        } catch (IllegalIndexShardStateException e) {
            logger.info("A shard is still not ready {{msg:{}}}", e.getMessage());
        }

        cachedGauges.put("indices.docs." + indexName + ".count", count);
        cachedGauges.put("indices.docs." + indexName + ".deleted", deleted);
    }

    private void updateIndicesIndexingStats(String indexName, IndexService service) {
        long count = 0;
        long time = 0;
        long current = 0;
        long deleted = 0;
        long deletedTime = 0;
        long deleteCurrent = 0;

        try {
            if (service != null) {
                for (IndexShard shard : service) {
                    IndexingStats.Stats stats = shard.indexingStats("_all").getTotal();
                    count += stats.getIndexCount();
                    time += stats.getIndexTimeInMillis();
                    current += stats.getIndexCurrent();

                    deleted += stats.getDeleteCount();
                    deletedTime += stats.getDeleteTimeInMillis();
                    deleteCurrent += stats.getDeleteCurrent();
                }
            }
        } catch (IllegalIndexShardStateException e) {
            logger.info("A shard is still not ready {{msg:{}}}", e.getMessage());
        }

        cachedGauges.put("indices.indexing." + indexName + ".index-count", count);
        cachedGauges.put("indices.indexing." + indexName + ".index-time", time);
        cachedGauges.put("indices.indexing." + indexName + ".index-current", current);
        cachedGauges.put("indices.indexing." + indexName + ".delete-count", deleted);
        cachedGauges.put("indices.indexing." + indexName + ".delete-time", deletedTime);
        cachedGauges.put("indices.indexing." + indexName + ".delete-current", deleteCurrent);
    }

    private void updateIndicesMergeStats(String indexName, IndexService service) {
        long time = 0;
        long count = 0;
        long docs = 0;
        long size = 0;
        long current = 0;

        try {
            if (service != null) {
                for (IndexShard shard : service) {
                    MergeStats stats = shard.mergeStats();
                    time += stats.getTotalTimeInMillis();
                    count += stats.getTotal();
                    docs += stats.getTotalNumDocs();
                    size += stats.getTotalSizeInBytes();
                    current += stats.getCurrent();
                }
            }
        } catch (IllegalIndexShardStateException e) {
            logger.info("A shard is still not ready {{msg:{}}}", e.getMessage());
        }

        cachedGauges.put("indices.merge." + indexName + ".time", time);
        cachedGauges.put("indices.merge." + indexName + ".count", count);
        cachedGauges.put("indices.merge." + indexName + ".docs", docs);
        cachedGauges.put("indices.merge." + indexName + ".size", size);
        cachedGauges.put("indices.merge." + indexName + ".current", current);
    }

    private void registerMetrics() {
        logger.info("registering [{}] metrics", cachedGauges.size());

        for (final String name : cachedGauges.keySet()) {
            metrics.register(name, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return cachedGauges.get(name);
                }
            });
        }

        alreadyRegistered = true;
    }

    private static long ratio(long time, long count) {
        if (count <= 0) return 0;

        return time / count;
    }
}
