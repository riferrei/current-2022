package current.session.riferrei;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang3.SerializationUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class WriterReader {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String TOPIC_NAME = "example-01";

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
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		try (KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(properties)) {

			byte[] bytes = SerializationUtils.serialize(
				new Person("Ricardo", 14,
					List.of("Marvel").toArray(new String[]{})));
	
			ProducerRecord<Integer, byte[]> record = new ProducerRecord<>(TOPIC_NAME, 1, bytes);

			producer.send(
				record, (recordMetadata, e) -> {
					System.out.printf("➡️ Message sent successfully to topic [%s] ✅\n", recordMetadata.topic());
				});

		}

	}

	private void read() {

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

		try (KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(properties)) {

			consumer.subscribe(Arrays.asList(TOPIC_NAME));

			for (;;) {
				ConsumerRecords<Integer, byte[]> messages = consumer.poll(Duration.ofSeconds(5));
				for (ConsumerRecord<Integer, byte[]> message : messages) {
					byte[] bytes = message.value();
					Person person = SerializationUtils.deserialize(bytes);
					System.out.printf("🧑🏻‍💻 userName: %s, favoriteNumber: %d, interests: %s\n",
						person.getUserName(), person.getFavoriteNumber(), Arrays.toString(person.getInterests()));
				}
			}

		}

	}

}
