public class BatchedLookupTransformer implements ValueTransformerWithKeySupplier<KeyType, ValueType, KeyValue<KeyType, ValueType>> {
    private ProcessorContext context;
    private KeyValueStore<KeyType, ValueType> stateStore;
    private int batchSize;
    private String matchedTopic;
    private String unmatchedTopic;

    public BatchedLookupTransformer(String stateStoreName, int batchSize, String matchedTopic, String unmatchedTopic) {
        this.batchSize = batchSize;
        this.matchedTopic = matchedTopic;
        this.unmatchedTopic = unmatchedTopic;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore<KeyType, ValueType>) context.getStateStore(stateStoreName);
    }

    @Override
    public KeyValue<KeyType, ValueType> transform(KeyType key, ValueType value) {
        List<ValueType> lookupValues = new ArrayList<>(batchSize);
        try (KeyValueIterator<KeyType, ValueType> iterator = stateStore.range(key, key)) {
            int count = 0;
            while (iterator.hasNext() && count < batchSize) {
                KeyValue<KeyType, ValueType> keyValue = iterator.next();
                lookupValues.add(keyValue.value);
                count++;
            }
        }

        if (!lookupValues.isEmpty()) {
            // Perform enrichment logic here
            ValueType enrichedValue = performEnrichment(value, lookupValues);

            // Forward the enriched record to the matched topic
            context.forward(key, enrichedValue, To.child(matchedTopic));
        } else {
            // Forward the unmatched record to the unmatched topic
            context.forward(key, value, To.child(unmatchedTopic));
        }

        // Return null to indicate that no output should be emitted to the downstream stream
        return null;
    }

    @Override
    public void close() {
        // Cleanup resources if necessary
    }

    @Override
    public ValueTransformer<KeyType, ValueType> get() {
        return this;
    }

    private ValueType performEnrichment(ValueType value, List<ValueType> lookupValues) {
        // Perform your enrichment logic here
        // You can access the original value and the lookup values to enrich the record
        // Return the enriched value
    }
}
