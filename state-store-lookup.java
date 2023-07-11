import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class StateStoreExample {

    public static void main(String[] args) {
        // Set up the Kafka Streams configuration
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-example");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, LookUpInstrumentAvroSerde.class.getName());

        // Build the Kafka Streams topology
        StreamsBuilder builder = new StreamsBuilder();

        // Get the state store and retrieve a value by key
        ReadOnlyKeyValueStore<String, LookUpInstrumentAvro> stateStore = getReadOnlyKeyValueStore("instrument_store", builder);
        String key = "some-key";
        LookUpInstrumentAvro value = stateStore.get(key);

        // Print a few values from the LookUpInstrument Avro
        if (value != null) {
            System.out.println("Key: " + key);
            System.out.println("Field1: " + value.getField1());
            System.out.println("Field2: " + value.getField2());
            // Add more fields to print if needed
        } else {
            System.out.println("Value not found for key: " + key);
        }

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private static ReadOnlyKeyValueStore<String, LookUpInstrumentAvro> getReadOnlyKeyValueStore(String storeName, StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, LookUpInstrumentAvro>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.String(),
                new LookUpInstrumentAvroSerde()
        );
        builder.addStateStore(storeBuilder);

        ReadOnlyKeyValueStore<String, LookUpInstrumentAvro> keyValueStore =
                builder.build().store(storeName, QueryableStoreTypes.keyValueStore());

        return keyValueStore;
    }
}
