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

            // Create the state store table
            KTable<String, InstrumentAvro> stateStoreTable = builder.table(
                    EnrichmentConfig.getStateStoreTopic(),
                    Consumed.with(Serdes.String(), avroSerde)
            );

            // Join the input stream with the state store table
            KStream<String, EnrichedTransactionAvro> enrichedStream = inputStream.leftJoin(
                    stateStoreTable,
                    (transaction, instrument) -> {
                        if (instrument != null) {
                            EnrichedTransactionAvro enrichedTransaction = new EnrichedTransactionAvro();

                            // Copy attributes from TransactionAvro to EnrichedTransactionAvro
                            enrichedTransaction.setId(transaction.getId());
                            enrichedTransaction.setAmount(transaction.getAmount());
                            enrichedTransaction.setNumber(transaction.getNumber());

                            // Copy attributes from InstrumentAvro to EnrichedTransactionAvro
                            enrichedTransaction.setName(instrument.getName());
                            enrichedTransaction.setValue(instrument.getValue());
                            enrichedTransaction.setCountry(instrument.getCountry());

                            return enrichedTransaction;
                        } else {
                            return null; // Return null for unmatched records
                        }
                    }
            );

            // Write the enriched stream to the output topic
            enrichedStream.to(
                    EnrichmentConfig.getOutputTopic(),
                    Produced.with(Serdes.String(), avroSerde)
            );

            // Write unmatched records to the park topic
            KStream<String, TransactionAvro> unmatchedStream = enrichedStream.filter((key, value) -> value == null);
            unmatchedStream.to(
                    EnrichmentConfig.getParkTopic(),
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
