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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * MultiThreadHLConsumer run =>
 * 	java MultiThreadHLConsumer multithreadtopic 4
 *
 *  zookeeper 실행 >
 * 	bin/zookeeper-server-start.sh config/zookeeper.properties
 *
 * 	broker 실행 >
 * 	env JMX_PORT=9999 bin/kafka-server-start.sh config/server-1.properties
 * 	env JMX_PORT=10000 bin/kafka-server-start.sh config/server-2.properties
 *
 * 	topic 생성 >
 * 	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 4 --topic multithreadtopic
 *
 *	producer 실행 >
 * 	bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093 --topic multithreadtopic
 */
public class MultiThreadHLConsumer {
	private ExecutorService executor;
	private final ConsumerConnector consumer;
	private final String topic;

	public MultiThreadHLConsumer(String zookeeper, String groupId, String topic) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");

		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		this.topic = topic;
	}

	class ConsumerThread implements Runnable {
		private KafkaStream stream;
		private int threadNumber;

		public ConsumerThread(KafkaStream stream, int threadNumber) {
			this.threadNumber = threadNumber;
			this.stream = stream;
		}

		public void run() {
			ConsumerIterator <byte[], byte[]> consumerIterator = stream.iterator();

			while (consumerIterator.hasNext()) {
				System.out.println("Message from thread :: " + threadNumber + " -- " + new String(consumerIterator.next().message()) );
				System.out.println("Shutting down Thread :: " + threadNumber);

			}
		}

	}

	public void testConsumer(int threadCount) {

		Map<String, Integer> topicCount = new HashMap<>();
		// 각각 토픽의 스레드 수를 정의한다
		topicCount.put(topic, new Integer(threadCount));
		// 여기서 단일 토픽을 사용했지만 topicCount 맵에 다중 토픽을 추가할 수 있다.
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStream = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[], byte[]>> streams = consumerStream.get(topic);
		// 스레드 풀 기동
		executor = Executors.newFixedThreadPool(threadCount);

		int threadNumber = 0;
		for(final KafkaStream stream : streams) {
			threadNumber ++;
			executor.submit(new ConsumerThread(stream, threadNumber));
		}

		try {
			Thread.sleep(100000);

		} catch (InterruptedException ie) {}

		if (consumer != null) {
			consumer.shutdown();
		}
		if(executor != null) {
			executor.shutdown();
		}
	}


	public static void main(String[] args) {
		String topic = args[0];
		int threadCount = Integer.parseInt(args[1]);
		MultiThreadHLConsumer multiThreadHLConsumer = new MultiThreadHLConsumer("localhost:2181", "testgroup", topic);
		multiThreadHLConsumer.testConsumer(threadCount);
	}

}
