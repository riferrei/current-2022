package current.session.riferrei;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;

import org.apache.kafka.common.serialization.IntegerSerializer;

public class Writer {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	private static final String TOPIC_NAME = "example-06";

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
		properties.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

		try (KafkaProducer<Integer, Person> producer = new KafkaProducer<>(properties)) {

			Person person = Person.newBuilder()
				.setUserName("Ricardo")
				.setFavoriteNumber(14)
				.addInterests("Marvel")
				.build();

			ProducerRecord<Integer, Person> record = new ProducerRecord<>(TOPIC_NAME, 1, person);
	
			producer.send(
				record, (recordMetadata, e) -> {
					System.out.printf("➡️ Message sent successfully to topic [%s] ✅\n", recordMetadata.topic());
				});
	
		}

	}

}
