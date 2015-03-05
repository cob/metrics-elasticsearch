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
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.service.NodeService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsWorker implements Runnable {

    private NodeService nodeService;
    private long interval;

    private boolean alreadyRegistered = false;

    public volatile boolean stopping;

    private Map<String, Long> cachedGauges = new ConcurrentHashMap<String, Long>();

    public MetricsWorker(NodeService nodeService, long interval) {
        this.nodeService = nodeService;
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
        cachedGauges.put("indices.docs.count", docs.getCount());
        cachedGauges.put("indices.docs.deleted", docs.getDeleted());

        SearchStats.Stats search = indices.getSearch().getTotal();
        cachedGauges.put("indices.search.total.query-time", search.getQueryTimeInMillis());
        cachedGauges.put("indices.search.total.query-count", search.getQueryCount());
        cachedGauges.put("indices.search.total.query-time-per",
                         ratio(search.getQueryTimeInMillis(), search.getQueryCount()));
        cachedGauges.put("indices.search.total.query-current", search.getQueryCurrent());
        cachedGauges.put("indices.search.total.fetch-time", search.getFetchTimeInMillis());
        cachedGauges.put("indices.search.total.fetch-count", search.getFetchCount());
        cachedGauges.put("indices.search.total.fetch-time-per",
                         ratio(search.getFetchTimeInMillis(), search.getFetchCount()));
        cachedGauges.put("indices.search.total.fetch-current", search.getFetchCurrent());

        IndexingStats.Stats indexing = indices.getIndexing().getTotal();
        cachedGauges.put("indices.indexing.total.index-time", indexing.getIndexTimeInMillis());
        cachedGauges.put("indices.indexing.total.index-count", indexing.getIndexCount());
        cachedGauges.put("indices.indexing.total.index-time-per",
                         ratio(indexing.getIndexTimeInMillis(), indexing.getIndexCount()));
        cachedGauges.put("indices.indexing.total.index-current", indexing.getIndexCurrent());
        cachedGauges.put("indices.indexing.total.delete-time", indexing.getDeleteTimeInMillis());
        cachedGauges.put("indices.indexing.total.delete-count", indexing.getDeleteCount());
        cachedGauges.put("indices.indexing.total.delete-time-per",
                         ratio(indexing.getDeleteTimeInMillis(), indexing.getDeleteCount()));
        cachedGauges.put("indices.indexing.total.delete-current", indexing.getDeleteCurrent());

        FlushStats flush = indices.getFlush();
        cachedGauges.put("indices.flush.time", flush.getTotalTimeInMillis());
        cachedGauges.put("indices.flush.count", flush.getTotal());
        cachedGauges.put("indices.flush.time-per",
                         ratio(flush.getTotalTimeInMillis(), flush.getTotal()));

        MergeStats merge = indices.getMerge();
        cachedGauges.put("indices.merge.time", merge.getTotalTimeInMillis());
        cachedGauges.put("indices.merge.count", merge.getTotal());
        cachedGauges.put("indices.merge.time-per",
                         ratio(merge.getTotalTimeInMillis(), merge.getTotal()));
        cachedGauges.put("indices.merge.docs", merge.getTotalNumDocs());
        cachedGauges.put("indices.merge.size", merge.getTotalSizeInBytes());
        //someday add current values

        RefreshStats refresh = indices.getRefresh();
        cachedGauges.put("indices.refresh.time", refresh.getTotalTimeInMillis());
        cachedGauges.put("indices.refresh.count", refresh.getTotal());
        cachedGauges.put("indices.refresh.time-per",
                         ratio(refresh.getTotalTimeInMillis(), refresh.getTotal()));

        SegmentsStats segments = indices.getSegments();
        cachedGauges.put("indices.segments.count", segments.getCount());
        cachedGauges.put("indices.segments.memory", segments.getMemoryInBytes());
        cachedGauges.put("indices.segments.index-writer-memory", segments.getIndexWriterMemoryInBytes());

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
