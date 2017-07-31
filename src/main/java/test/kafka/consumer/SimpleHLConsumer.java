package test.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * SimpleHLConsumer run =>
 * 	java SimpleHLConsumer kafkatopic
 *
 * bin/zookeeper-server-start.sh config/zookeeper.properties
 * bin/kafka-server-start.sh config/server.properties
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic simpletopic
 *
 */
public class SimpleHLConsumer {

	private final ConsumerConnector consumer;
	private final String topic;

	public SimpleHLConsumer(String zookeeper, String groupId, String topic) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");

		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		this.topic = topic;

	}

	public void testConsumer() {
		Map<String, Integer> topicCount = new HashMap<>();

		// 토픽에 단일 쓰레드 배정
		topicCount.put(topic, new Integer(1));
		Map<String, List<KafkaStream <byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);

		for(final KafkaStream stream : streams) {
			ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
			while(consumerIterator.hasNext()) {
				System.out.println("Message from Single Topic :: " + new String(consumerIterator.next().message()));

			}
		}

		if(consumer != null) {
			consumer.shutdown();
		}
	}

	public static void main(String[] args) {
		String topic = args[0];
		SimpleHLConsumer simpleHLConsumer = new SimpleHLConsumer("localhost:2181", "testgroup", topic);
		simpleHLConsumer.testConsumer();
	}
}
