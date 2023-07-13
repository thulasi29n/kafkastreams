                           +----------------------+
                           |                      |
                           |     Kafka Topic      |
                           |                      |
                           +----------+-----------+
                                      |
                                      |
                                      v
                             +-----------------+
                             |                 |
                             |  Kafka Streams  |
                             |    Application  |
                             |                 |
                             +--------+--------+
                                      |
                                      |
                                      v
                           +----------------------+
                           |                      |
                           |     Input Stream     |
                           |                      |
                           +----------+-----------+
                                      |
                                      |
                                      v
                          +---------------------+
                          |                     |
                          |  Partial Key Lookup |
                          |    Transformer      |
                          |                     |
                          +---------+-----------+
                                    |
                                    |
                                    v
                          +---------------------+
                          |                     |
                          | Transformed Stream  |
                          |                     |
                          +---------+-----------+
                                    |
                                    |
                                    v
                          +---------------------+
                          |                     |
                          |     Process/Output  |
                          |                     |
                          +---------------------+


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class PartialKeyLookupExample {

    public static void main(String[] args) {
        // Configure the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "partial-key-lookup-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create the Kafka Streams builder
        StreamsBuilder builder = new StreamsBuilder();

        // Create the input stream
        KStream<String, String> inputStream = builder.stream("input-topic");

        // Perform custom lookups using transform()
        KStream<String, String> transformedStream = inputStream.transform(
                () -> new PartialKeyLookupTransformer("userStateStore"),
                Named.as("partial-key-lookup-transformer")
        );

        // Process the transformed stream
        transformedStream.foreach((key, value) -> {
            // Process the transformed records
            System.out.println("Key: " + key + ", Value: " + value);
        });

        // Build the topology
        Topology topology = builder.build();

        // Create the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(topology, props);

        // Start the Kafka Streams application
        streams.start();

        // Gracefully shut down the application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static class PartialKeyLookupTransformer implements TransformerSupplier<String, String, KeyValue<String, String>> {

        private final String stateStoreName;

        public PartialKeyLookupTransformer(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public Transformer<String, String, KeyValue<String, String>> get() {
            return new Transformer<String, String, KeyValue<String, String>>() {

                private KeyValueStore<String, String> stateStore;

                @Override
                public void init(ProcessorContext context) {
                    stateStore = (KeyValueStore<String, String>) context.getStateStore(stateStoreName);
                }

                @Override
                public KeyValue<String, String> transform(String key, String value) {
                    // Perform custom lookup based on partial keys
                    String partialKey = key.substring(0, partialKeyLength); // Modify as per your partial key length requirement
                    String lookupResult = stateStore.get(partialKey);

                    // Return the transformed record with the lookup result
                    return KeyValue.pair(key, lookupResult);
                }

                @Override
                public void close() {
                    // Clean up any resources if needed
                }
            };
        }
    }
}
