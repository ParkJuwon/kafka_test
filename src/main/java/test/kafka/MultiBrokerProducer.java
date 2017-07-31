package test.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;


/**
 * topic create =>
 *  bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 5 --topic kafkatopic
 *
 * MultiBrokerProducer run =>
 *  java MultiBrokerProducer kafkatopic
 *
 * Consumer Test =>
 * 	bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic kafkatopic --from-beginning
 */
public class MultiBrokerProducer {
	private static Producer<String, String> producer;
	private final Properties props = new Properties();

	public MultiBrokerProducer() {
		props.put("metadata.broker.list", "localhost:9092, localhost:9093");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("partitioner.class", "test.kafka.SimplePartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	// 토픽을 지울때 server.properties 의 delete.topic.enable=true 속성 주석 제
	public static void main(String[] args) {
		MultiBrokerProducer sp = new MultiBrokerProducer();
		Random rnd = new Random();
		String topic = args[0];

		for (long messCount = 0; messCount < 10; messCount++) {
			Integer key = rnd.nextInt(255);
			String msg = "This message is for key - " + key;
			KeyedMessage<String, String> data1 = new KeyedMessage<String, String>(topic,
					String.valueOf(key),
					msg);

			producer.send(data1);
		}

		producer.close();
	}

}
