StreamsBuilder builder = new StreamsBuilder();

// Define your input topic
KStream<String, String> inputTopic = builder.stream("input-topic");

// Apply selectKey() operation to extract the desired key portion
KStream<String, String> transformedStream = inputTopic.selectKey((key, value) -> {
    // Extract desired key portion
    String partialKey = key.substring(0, 5); // Example: Extract first 5 characters
    return partialKey;
});

// Create an intermediate topic for the transformed stream
String intermediateTopic = "intermediate-topic";

// Write the transformed stream to the intermediate topic
transformedStream.to(intermediateTopic);

// Apply partition key extraction to the intermediate topic
builder.stream(intermediateTopic)
       .partitionedBy((key, value, recordContext) -> {
           // Extract desired key portion for partitioning
           String partialKey = key.substring(0, 5); // Example: Extract first 5 characters
           return partialKey;
       })
       .map((key, value) -> KeyValue.pair(key + "+" + value, value)) // Append full key to the value
       .to("output-topic");

// Build and start the Kafka Streams application
KafkaStreams streams = new KafkaStreams(builder.build(), config);
streams.start();
