package cn.celloud.bf.stormProject1.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import cn.celloud.bf.stormProject1.constant.Constant;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class OrderConsumer extends Thread {
	private ConsumerConnector consumer;
	private String topic;
	
	private Queue<String> queue = new ConcurrentLinkedQueue<String>();
	
	public OrderConsumer(String topic){
		this.topic = topic;
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
	}
	
	private ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", Constant.ZK_CONNECT);
		props.put("group.id", Constant.GROUP_ID);
		//zookeeper的心跳超时时间，查过这个时间就认为是无效的消费者
		props.put("zookeeper.session.timeout.ms", Constant.ZK_SESSION_TIMEOUT_MS);
		//zookeeper的follower同leader的同步时间
		props.put("zookeeper.sync.time.ms", Constant.ZK_SYNC_TIME_MS);
		//是否自动提交
		props.put("auto.commit.enable", Constant.AUTO_COMMIT);
		//自动提交的时间间隔
		props.put("auto.commit.interval.ms", Constant.AUTO_COMMIT_INTERVAL);
		return new ConsumerConfig(props);
	}
	
	@Override
	public void run() {
		HashMap<String, Integer> topicCountMap = new HashMap<String,Integer>();
		//key:topic名称  value：同一个消费者组中，几个线程/几个消费者去消费
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = 
				consumer.createMessageStreams(topicCountMap);
		//正常情况下，get(topic)返回list集合，size表示有几个消费者线程，目前我们设定1个消费者，所以，直接get(0)即可
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while(it.hasNext()){
			//逻辑处理
			String message = new String(it.next().message());
			queue.add(message);
//			System.out.println("consumer:"+message);
		}
	}
	
	public Queue getQueue(){
		return queue;
	}
	public static void main(String[] args) {
		OrderConsumer orderConsumer = new OrderConsumer(Constant.ORDER_TOPIC);
		orderConsumer.start();
	}
}
