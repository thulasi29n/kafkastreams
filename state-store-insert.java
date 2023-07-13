import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.*;

import java.util.Properties;

public class StateStoreBatchProcessingExample {

    public static void main(String[] args) {
        // Configure the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-batch-processing-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Set the desired batch size
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024); // 10 MB

        // Create the Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a state store and add it to the builder
        StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("myStateStore"),
                        Serdes.String(),
                        Serdes.String());
        builder.addStateStore(storeBuilder);

        // Process the input topic and populate the state store
        builder.stream("input-topic")
                .foreach((key, value) -> {
                    // Access the state store
                    KeyValueStore<String, String> stateStore =
                            (KeyValueStore<String, String>) context().getStateStore("myStateStore");

                    // Populate the state store with the key-value pair from the input topic
                    stateStore.put(key, value);
                });

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
