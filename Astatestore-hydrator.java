public class StateStoreHydrator implements Processor<String, InstrumentAvro> {
    private KeyValueStore<String, InstrumentAvro> stateStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore<String, InstrumentAvro>) context.getStateStore("your-state-store");
    }

    @Override
    public void process(String key, InstrumentAvro value) {
        // Hydrate the state store with the key-value pair
        stateStore.put(key, value);
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }
}
Properties props = new Properties();
// Set Kafka Streams configuration properties

StreamsBuilder builder = new StreamsBuilder();
String inputTopic = "your-input-topic";
String stateStoreName = "your-state-store";

// Create the input stream from the topic
KStream<String, InstrumentAvro> inputStream = builder.stream(inputTopic);

// Define the state store
StoreBuilder<KeyValueStore<String, InstrumentAvro>> storeBuilder =
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(stateStoreName),
                Serdes.String(),
                InstrumentAvroSerdeFactory.create()
        );

// Add the state store to the topology
builder.addStateStore(storeBuilder);

// Process the input stream with the StateStoreHydrator processor
inputStream.process(() -> new StateStoreHydrator(), stateStoreName);

// Build and start the Kafka Streams application
KafkaStreams streams = new KafkaStreams(builder.build(), props);
streams.start();
