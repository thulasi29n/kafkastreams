import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class TopologyBuilder {
    public static Topology buildTopology(StreamsBuilder builder) {
        // Define the topology
        KStream<String, Transaction> inputStream = builder.stream("input-topic");
        
        // Add your processing logic and transformations
        KStream<String, EnrichedTransaction> enrichedStream = inputStream.transform(() -> new Transformer<String, Transaction, KeyValue<String, EnrichedTransaction>>() {
            // Your transformation logic
        });

        enrichedStream.to("output-topic", Produced.with(Serdes.String(), getEnrichedTransactionAvroSerde()));

        // Build and return the topology
        return builder.build();
    }
}


import org.apache.kafka.streams.*;
import java.util.Properties;

public class StateStoreEnrichmentExample {
    public static void main(String[] args) {
        Properties config = KafkaConfig.getProperties();

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = TopologyBuilder.buildTopology(builder);

        KafkaStreams streams = new KafkaStreams(topology, config);
		System.out.println(topology.describe());

        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
