public class StateStorePopulator {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-populator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, String>> stateStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("my-state-store"),
                Serdes.String(),
                Serdes.String()
        );

        builder.addStateStore(stateStoreBuilder);

        KStream<String, String> instrumentStream = builder.stream("instrument-topic");
        instrumentStream.foreach((key, value) -> {
            // Populate the state store with instrument data
            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();
            ProcessorContext context = streams.getGlobalProcessorContext();
            KeyValueStore<String, String> stateStore = (KeyValueStore<String, String>) context.getStateStore("my-state-store");
            stateStore.put(key, value);
            streams.close();
        });

        KStream<String, String> transactionStream = builder.stream("transaction-topic");
        transactionStream.transformValues(() -> new MyTransformer("my-state-store"))
                .to("enriched-transaction-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static class MyTransformer implements ValueTransformerSupplier<String, String> {
        private final String stateStoreName;

        public MyTransformer(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public ValueTransformer<String, String> get() {
            return new ValueTransformer<String, String>() {
                private KeyValueStore<String, String> stateStore;

                @Override
                public void init(ProcessorContext context) {
                    stateStore = (KeyValueStore<String, String>) context.getStateStore(stateStoreName);
                }

                @Override
                public String transform(String value) {
                    // Perform transformation using the state store
                    // Retrieve values from state store based on your logic
                    String transformedValue = stateStore.get(/* key based on your logic */);
                    // Perform transformation on value using the retrieved data
                    String enrichedValue = transformValue(value, transformedValue);
                    return enrichedValue;
                }

                @Override
                public void close() {
                    // Clean up resources if necessary
                }
            };
        }
    }

    private static String transformValue(String value, String transformedValue) {
        // Perform your transformation logic here
        // Modify the value using the retrieved transformedValue
        // Return the transformed value
        return value;
    }
}
