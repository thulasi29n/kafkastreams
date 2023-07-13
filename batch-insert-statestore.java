import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StateStoreBatchProcessingExample {

    public static void main(String[] args) {
        // Configure the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-batch-processing-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create the Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a state store and add it to the builder
        StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("myStateStore"),
                        Serdes.String(),
                        Serdes.String());
        builder.addStateStore(storeBuilder);

        // Process the input topic and populate the state store in batches
        int batchSize = 1000;
        builder.stream("input-topic")
                .process(() -> new BatchingProcessor(batchSize), "myStateStore");

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private static class BatchingProcessor implements Processor<String, String> {
        private final int batchSize;
        private ProcessorContext context;
        private List<KeyValue<String, String>> batch;

        public BatchingProcessor(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.batch = new ArrayList<>();
        }

        @Override
        public void process(String key, String value) {
            batch.add(new KeyValue<>(key, value));

            if (batch.size() >= batchSize) {
                populateStateStore();
            }
        }

        private void populateStateStore() {
            KeyValueStore<String, String> stateStore =
                    (KeyValueStore<String, String>) context.getStateStore("myStateStore");

            for (KeyValue<String, String> record : batch) {
                stateStore.put(record.key, record.value);
            }

            batch.clear();
        }

        @Override
        public void close() {
            // Process any remaining records in the last batch
            if (!batch.isEmpty()) {
                populateStateStore();
            }
        }
    }
}
