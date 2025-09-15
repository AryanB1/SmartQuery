package smartquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import smartquery.storage.ColumnStore;
import smartquery.ingest.IngestService;
import smartquery.ingest.kafka.KafkaIngestConsumer;
import smartquery.query.QueryService;
import smartquery.index.IndexManager;
import smartquery.metrics.MetricsCollector;

@Configuration
public class SmartQueryConfig {

    @Bean
    public MetricsCollector metricsCollector() {
        return new MetricsCollector();
    }

    @Bean
    public ColumnStore columnStore() {
        return new ColumnStore();
    }

    @Bean
    public IngestService ingestService(ColumnStore columnStore) {
        return new IngestService(columnStore);
    }

    @Bean
    public QueryService queryService(ColumnStore columnStore) {
        return new QueryService(columnStore);
    }

    @Bean
    public IndexManager indexManager() {
        return new IndexManager();
    }

    @Bean
    public KafkaIngestConsumer kafkaIngestConsumer(IngestService ingestService) {
        KafkaIngestConsumer consumer = new KafkaIngestConsumer(ingestService);
        consumer.start();
        return consumer;
    }
}
