package current.session.riferrei;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class Writer {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String TOPIC_NAME	 = "example-03";

	public static void main(String[] args) {

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

}
