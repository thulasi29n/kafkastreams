import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@Data
public class KafkaConfig {

    @Value("${spring.kafka.streams.input-topic}")
    private String inputTopic;

    @Value("${spring.kafka.streams.output-topic}")
    private String outputTopic;

    @Value("${spring.kafka.streams.state-store-name}")
    private String stateStoreName;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    // Other Kafka Streams properties...

}
