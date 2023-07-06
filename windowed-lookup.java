import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class AvroEnrichmentApplication {

    public static void main(String[] args) {
        // Initialize Kafka Streams configuration
        Properties config = new Properties();
        // Set the necessary configuration properties

        StreamsBuilder builder = new StreamsBuilder();

        // Create the input stream
        KStream<String, TransactionAvro> inputStream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), avroSerde)
        );

        // Repartition the input stream based on the new key
        KStream<String, TransactionAvro> repartitionedStream = inputStream.selectKey(
                (key, value) -> transformKey(value)
        );

        // Enrich the repartitioned stream using the state store
        KStream<String, EnrichedTransactionAvro> enrichedStream = repartitionedStream.transformValues(
                EnrichmentTransformer::new,
                "my-state-store"
        );

        // Write the enriched stream to the output topic
        enrichedStream.to(
                "output-topic",
                Produced.with(Serdes.String(), avroSerde)
        );

        // Build the Kafka Streams topology
        Topology topology = builder.build();

        // Print the topology description
        System.out.println(topology.describe());

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(topology, config);

        // Add a shutdown hook for graceful application shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }

    // Transformation logic for generating the new key
    private static String transformKey(TransactionAvro transaction) {
        return transaction.getIsin() + transaction.getTradeDate();
    }

    // Rest of the code remains the same as before
}
