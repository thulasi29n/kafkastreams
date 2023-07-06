import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.example.avro.TransactionAvro;
import com.example.avro.InstrumentAvro;
import com.example.avro.EnrichedInstrumentAvro;

public class EnrichmentTransformer implements ValueTransformer<TransactionAvro, EnrichedInstrumentAvro> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public EnrichedInstrumentAvro transform(TransactionAvro transactionAvro) {
        // Get the key from the transformed record
        String key = transactionAvro.getNewKey(); // Replace with the actual attribute or logic for the new key

        // Perform the lookup in the state store for the key
        GenericRecord stateRecord = (GenericRecord) context.getStateStore("my-state-store").get(key);

        if (stateRecord != null) {
            // Create a new EnrichedInstrumentAvro object
            EnrichedInstrumentAvro enrichedInstrumentAvro = new EnrichedInstrumentAvro();

            // Copy attributes from TransactionAvro to EnrichedInstrumentAvro
            for (Schema.Field field : TransactionAvro.getClassSchema().getFields()) {
                String attributeName = field.name();

                try {
                    Object attributeValue = transactionAvro.get(attributeName);
                    enrichedInstrumentAvro.put(attributeName, attributeValue);
                } catch (AvroRuntimeException e) {
                    // Handle any exceptions or missing attributes as needed
                }
            }

            // Set attributes from InstrumentAvro
            enrichedInstrumentAvro.setInstrumentId(stateRecord.get("instrumentId"));
            enrichedInstrumentAvro.setInstrumentName(stateRecord.get("instrumentName"));
            enrichedInstrumentAvro.setInstrumentValue(stateRecord.get("instrumentValue"));
            // Set other attributes from stateRecord

            return enrichedInstrumentAvro;
        }

        return null; // Return null for non-matching records
    }

    @Override
    public void close() {
        // Clean up any resources if needed
    }
}
