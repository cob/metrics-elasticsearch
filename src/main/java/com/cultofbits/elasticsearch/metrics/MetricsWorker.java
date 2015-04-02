package com.cultofbits.elasticsearch.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.service.NodeService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsWorker implements Runnable {

    private NodeService nodeService;
    private IndicesService indicesService;
    private long interval;

    private boolean alreadyRegistered = false;

    public volatile boolean stopping;

    private Map<String, Long> cachedGauges = new ConcurrentHashMap<>();

    public MetricsWorker(NodeService nodeService, IndicesService indicesService, long interval) {
        this.nodeService = nodeService;
        this.indicesService = indicesService;
        this.interval = interval;
    }


    @Override
    public void run() {
        while (!stopping) {
            try {
                Thread.sleep(interval);
                updateStats();

                if(!alreadyRegistered) registerMetrics();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    void updateStats() {
        NodeStats stats = nodeService.stats();

        NodeIndicesStats indices = stats.getIndices();

        DocsStats docs = indices.getDocs();
        cachedGauges.put("indices.docs._all.count", docs.getCount());
        cachedGauges.put("indices.docs._all.deleted", docs.getDeleted());

        updateIndicesDocsStats();

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

        updateIndicesIndexingStats();

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
        //someday add current values

        updateIndicesMergeStats();

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

    private void updateIndicesDocsStats() {
        for (IndexService service : indicesService) {
            long count = 0;
            long deleted = 0;

            for (IndexShard shard : service) {
                DocsStats stats = shard.docStats();
                count += stats.getCount();
                deleted += stats.getDeleted();
            }

            cachedGauges.put("indices.docs." + service.index().name() + ".count", count);
            cachedGauges.put("indices.docs." + service.index().name() + ".deleted", deleted);
        }
    }

    private void updateIndicesIndexingStats() {
        for (IndexService service : indicesService) {
            long count = 0;
            long time = 0;
            long deleted = 0;
            long deletedTime = 0;

            for (IndexShard shard : service) {
                IndexingStats.Stats stats = shard.indexingStats("_all").getTotal();
                count += stats.getIndexCount();
                time += stats.getIndexTimeInMillis();
                deleted += stats.getDeleteCount();
                deletedTime += stats.getDeleteTimeInMillis();
            }

            cachedGauges.put("indices.indexing." + service.index().name() + ".index-count", count);
            cachedGauges.put("indices.indexing." + service.index().name() + ".index-time", time);
            cachedGauges.put("indices.indexing." + service.index().name() + ".delete-count", deleted);
            cachedGauges.put("indices.indexing." + service.index().name() + ".delete-time", deletedTime);
        }
    }

    private void updateIndicesMergeStats() {
        for (IndexService service : indicesService) {
            long time = 0;
            long count = 0;
            long docs = 0;
            long size = 0;

            for (IndexShard shard : service) {
                MergeStats stats = shard.mergeStats();
                time += stats.getTotalTimeInMillis();
                count += stats.getTotal();
                docs += stats.getTotalNumDocs();
                size += stats.getTotalSizeInBytes();
            }

            cachedGauges.put("indices.merge." + service.index().name() + ".time", time);
            cachedGauges.put("indices.merge." + service.index().name() + ".count", count);
            cachedGauges.put("indices.merge." + service.index().name() + ".docs", docs);
            cachedGauges.put("indices.merge." + service.index().name() + ".size", size);
        }
    }

    private void registerMetrics() {
        MetricRegistry metrics = SharedMetricRegistries.getOrCreate("elasticsearch");

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

    private static long ratio(long time, long count){
        if(count <= 0) return 0;

        return time / count;
    }
}
