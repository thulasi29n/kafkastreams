import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StateStoreExample {

    public static void main(String[] args) {
        // Set up the Kafka Streams configuration
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-example");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, LookUpInstrumentAvroSerde.class.getName());

        // Build the state store
        StoreBuilder<KeyValueStore<String, LookUpInstrumentAvro>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("instrument_store"),
                        Serdes.String(),
                        new LookUpInstrumentAvroSerde());

        // Create the state store
        ReadOnlyKeyValueStore<String, LookUpInstrumentAvro> keyValueStore =
                storeBuilder.build();

        // Open the state store
        keyValueStore.init(null, null);

        // Get the value from the state store using the key
        String key = "some-key";
        LookUpInstrumentAvro value = keyValueStore.get(key);

        // Print a few values from the LookUpInstrument Avro
        if (value != null) {
            System.out.println("Key: " + key);
            System.out.println("Field1: " + value.getField1());
            System.out.println("Field2: " + value.getField2());
            // Add more fields to print if needed
        } else {
            System.out.println("Value not found for key: " + key);
        }

        // Close the state store
        keyValueStore.close();
    }
}
