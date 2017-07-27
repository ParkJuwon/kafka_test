package test.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class SimpleProducer {
	private static KafkaProducer<Integer, String> producer;
	private final Properties props = new Properties();

	public SimpleProducer() {
//		props.put("metadata.broker.list", "localhost:9092");
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("bootstrap.servers", "localhost:9092");


		//org.apache.kafka.common.serialization.StringSerializer
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Integer, String>(props);
	}

	public static void main(String[] args) {
		SimpleProducer sp = new SimpleProducer();
		String topic = (String) args[0];
		String messageStr = (String) args[1];
		ProducerRecord<Integer, String> data = new ProducerRecord<Integer, String>(topic, messageStr);

		producer.send(data);
		producer.close();
	}

}
