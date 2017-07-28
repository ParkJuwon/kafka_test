package test.kafka;


import kafka.producer.Partitioner;

public class SimplePartitioner implements Partitioner {

	@Override
	public int partition(Object key, int numPartitions) {
		int partition = 0;
		int iKey = Integer.parseInt(String.valueOf(key));

		if(iKey > 0) {
			partition = iKey % numPartitions;
		}

		return partition;
	}
}
