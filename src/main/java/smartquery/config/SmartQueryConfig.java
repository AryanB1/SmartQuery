package smartquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import smartquery.storage.ColumnStore;
import smartquery.ingest.IngestService;
import smartquery.ingest.kafka.KafkaIngestConsumer;

@Configuration
public class SmartQueryConfig {

    @Bean
    public ColumnStore columnStore() {
        return new ColumnStore();
    }

    @Bean
    public IngestService ingestService(ColumnStore columnStore) {
        return new IngestService(columnStore);
    }

    @Bean
    public KafkaIngestConsumer kafkaIngestConsumer(IngestService ingestService) {
        KafkaIngestConsumer consumer = new KafkaIngestConsumer(ingestService);
        consumer.start();
        return consumer;
    }
}
