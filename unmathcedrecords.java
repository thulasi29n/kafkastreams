import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Properties;

public class AvroEnrichmentApplication {

    public static void main(String[] args) {
        try {
            Properties config = EnrichmentConfig.getConfig();
            config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, CustomExceptionHandler.class);
            config.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, CustomExceptionHandler.class);

            StreamsBuilder builder = new StreamsBuilder();

            // Create the input stream
            KStream<String, TransactionAvro> inputStream = builder.stream(
                    EnrichmentConfig.getInputTopic(),
                    Consumed.with(Serdes.String(), avroSerde)
            );

            // Rest of the code remains the same
            // ...

            // Enrich the input stream and handle unmatched records
            KStream<String, EnrichedTransactionAvro> enrichedStream = inputStream.transformValues(
                    EnrichmentTransformer::new,
                    "my-state-store"
            ).orElseStream().to(
                    "enrichment_park_txns",
                    Produced.with(Serdes.String(), avroSerde)
            );

            // Write the enriched stream to the output topic
            enrichedStream.to(
                    EnrichmentConfig.getOutputTopic(),
                    Produced.with(Serdes.String(), avroSerde)
            );

            // Build and start the Kafka Streams application
            KafkaStreams streams = new KafkaStreams(builder.build(), config);
            streams.start();

            // Add a shutdown hook for graceful application shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        } catch (Exception e) {
            e.printStackTrace();
            // Handle the exception appropriately, e.g., log the error, send alerts, or perform cleanup tasks
        }
    }

    // Rest of the code remains the same
    // ...

    public static class CustomExceptionHandler implements DeserializationExceptionHandler, ProductionExceptionHandler {

        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
            // Handle deserialization exception
            // Implement custom logic to handle the exception, such as logging, error handling, or returning a response
            return DeserializationHandlerResponse.CONTINUE;
        }

        @Override
        public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
            // Handle production exception
            // Implement custom logic to handle the exception, such as logging, error handling, or returning a response
            return ProductionExceptionHandlerResponse.CONTINUE;
        }
    }
}
