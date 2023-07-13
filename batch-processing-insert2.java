                                        +-------------------+
                                        |                   |
                                        |   Kafka Topic     |
                                        |                   |
                                        +-------------------+
                                                  |
                                                  |
                                                  v
                                        +-------------------+
                                        |                   |
                                        |   Kafka Streams   |
                                        |      App          |
                                        |                   |
                                        +--------+----------+
                                                 |
                                                 |
                   +-------------------------------v-------------------------------+
                   |                                                               |
                   |                 State Store (userStateStore)                   |
                   |                                                               |
                   |   +-------------------------------------------------------+   |
                   |   |                                                       |   |
                   |   |                Partial Key Index (userPartialKeyIndex)   |   |
                   |   |                                                       |   |
                   |   +-------------------------------------------------------+   |
                   |                                                               |
                   +---------------------------------------------------------------+
                                                 |
                                                 |
                                                 v
                                        +-------------------+
                                        |                   |
                                        |   Partial Key     |
                                        |    Lookup         |
                                        |                   |
                                        +--------+----------+
                                                 |
                                                 |
                                                 v
                                        +-------------------+
                                        |                   |
                                        |   Matching Keys   |
                                        |                   |
                                        +--------+----------+
                                                 |
                                                 |
                                                 v
                                        +-------------------+
                                        |                   |
                                        |   State Store     |
                                        |     Lookup        |
                                        |                   |
                                        +--------+----------+
                                                 |
                                                 |
                                                 v
                                        +-------------------+
                                        |                   |
                                        |   Retrieved      |
                                        |   User Profiles   |
                                        |                   |
                                        +-------------------+


import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.*;

import java.util.*;

public class PartialKeyLookupExample {

    public static void main(String[] args) {
        // Configure the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "partial-key-lookup-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create the Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Create a state store and add it to the builder
        StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("userStateStore"),
                        Serdes.String(),
                        Serdes.String());
        builder.addStateStore(storeBuilder);

        // Build the topology
        Topology topology = builder.build();

        // Create the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(topology, props);

        // Initialize the state store index during startup or scheduled updates
        initializePartialKeyIndex(streams, "userStateStore", "userPartialKeyIndex");

        // Start the Kafka Streams application
        streams.start();

        // Perform partial key lookups
        performPartialKeyLookup(streams, "userStateStore", "userPartialKeyIndex", "prefix");

        // Gracefully shut down the application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void initializePartialKeyIndex(KafkaStreams streams, String stateStoreName, String indexStoreName) {
        KeyValueStore<String, String> stateStore = streams.store(stateStoreName, QueryableStoreTypes.keyValueStore());

        // Create a map to store partial key to key mappings
        Map<String, List<String>> partialKeyIndex = new HashMap<>();

        // Iterate over the state store and build the index
        KeyValueIterator<String, String> iterator = stateStore.all();
        while (iterator.hasNext()) {
            KeyValue<String, String> keyValue = iterator.next();
            String key = keyValue.key;
            String partialKey = key.substring(0, prefixLength); // Modify as per your partial key length requirement

            // Update the index with the partial key mapping
            partialKeyIndex.computeIfAbsent(partialKey, k -> new ArrayList<>()).add(key);
        }
        iterator.close();

        // Store the partial key index in a separate store
        KeyValueStore<String, List<String>> indexStore = streams.store(indexStoreName, QueryableStoreTypes.keyValueStore());
        partialKeyIndex.forEach(indexStore::put);
    }

    private static void performPartialKeyLookup(KafkaStreams streams, String stateStoreName, String indexStoreName, String partialKey) {
        KeyValueStore<String, List<String>> indexStore = streams.store(indexStoreName, QueryableStoreTypes.keyValueStore());
        KeyValueStore<String, String> stateStore = streams.store(stateStoreName, QueryableStoreTypes.keyValueStore());

        // Lookup the partial key in the index
        List<String> matchingKeys = indexStore.get(partialKey);

        if (matchingKeys != null) {
            // Process the matching keys and retrieve the corresponding values from the state store
            for (String key : matchingKeys) {
                String value = stateStore.get(key);
                System.out.println("Key: " + key + ", Value: " + value);
            }
        }
    }
}
