import java.util.Properties;

public class KafkaConfig {
    public static Properties getProperties() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-enrichment-example");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/path/to/state-store-directory");
        // Add more Kafka configuration properties as needed
        return config;
    }
}


public class StateStoreEnrichmentExample {
    public static void main(String[] args) {
        Properties config = KafkaConfig.getProperties();

        // Rest of your Kafka Streams application code using the 'config' properties
        // ...
    }
}


