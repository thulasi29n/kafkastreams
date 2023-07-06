import java.util.Properties;

public class EnrichmentConfig {
    public static Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Add other necessary configuration properties
        return config;
    }

    public static String getInputTopic() {
        return "input-topic";
    }

    public static String getOutputTopic() {
        return "output-topic";
    }
}


public class AvroEnrichmentApplication {

    public static void main(String[] args) {
        Properties config = EnrichmentConfig.getConfig();

        StreamsBuilder builder = new StreamsBuilder();

        // Create the input stream
        KStream<String, TransactionAvro> inputStream = builder.stream(
                EnrichmentConfig.getInputTopic(),
                Consumed.with(Serdes.String(), avroSerde)
        );

        // Rest of the code remains the same
        // ...
        
        // Write the enriched stream to the output topic
        enrichedStream.to(
                EnrichmentConfig.getOutputTopic(),
                Produced.with(Serdes.String(), avroSerde)
        );

        // Rest of the code remains the same
        // ...
    }

    // Rest of the code remains the same
    // ...
}
