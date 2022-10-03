package current.session.riferrei;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

// import software.amazon.awssdk.services.glue.model.DataFormat;
// import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
// import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
// import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
// import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;

public class WriterReader {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	private static final String TOPIC_NAME = "example-07";

	public static void main(String[] args) {
		String oper = args[0];
		if (oper.equalsIgnoreCase("w")) {
			new WriterReader().write();
		}
		if (oper.equalsIgnoreCase("r")) {
			new WriterReader().read();
		}
	}

	private void write() {

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

		properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

		// properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
		// properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
		// properties.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
		// properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "test-2022-4");
		// properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "person.avsc");
		// properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
		// properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());

		try (KafkaProducer<Integer, Person> producer = new KafkaProducer<>(properties)) {

			Person person = Person.newBuilder()
				.setUserName("Ricardo")
				.setFavoriteNumber(14L)
				.setInterests(List.of("Marvel"))
				.build();
	
			ProducerRecord<Integer, Person> record = new ProducerRecord<>(TOPIC_NAME, 1, person);

			producer.send(
				record, (recordMetadata, e) -> {
					System.out.printf("➡️ Message sent successfully to topic [%s] ✅\n", recordMetadata.topic());
				});

		}

	}

	private void read() {

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
		
		// properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
		// properties.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
		// properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "test-2022-4");
		// properties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());

		// properties.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, KafkaAvroDeserializer.class.getName());
		// properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		// properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

		try (KafkaConsumer<Integer, Person> consumer = new KafkaConsumer<>(properties)) {

			consumer.subscribe(Arrays.asList(TOPIC_NAME));

			for (;;) {
				ConsumerRecords<Integer, Person> messages = consumer.poll(Duration.ofSeconds(5));
				for (ConsumerRecord<Integer, Person> message : messages) {
					Person person = message.value();
					System.out.printf("🧑🏻‍💻 userName: %s, favoriteNumber: %d, interests: %s\n",
						person.getUserName(), person.getFavoriteNumber(), person.getInterests());
				}
			}

		}

	}

}
