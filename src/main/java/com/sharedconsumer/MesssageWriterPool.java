package com.sharedconsumer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MesssageWriterPool<K,V> extends LinkedBlockingQueue<ConsumerRecord<K,V>>  {
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(MesssageWriterPool.class);

	private int duration;
	private int poolSize;
	private OffsetManager offsetManager;
	private ExecutorService executorService;
	private KafkaConsumer<K,V> consumer;
	private HashMap<K,ConsumerRecord<K,V>> pendings;

	
	public MesssageWriterPool( int poolSize, int duration, KafkaConsumer<K,V> consumer) {
			
		this.duration = duration;
		this.poolSize = poolSize;
		this.consumer = consumer;
		this.pendings = new HashMap<>();	
		this.offsetManager = new OffsetManager();
	}
	
	public void start() {
		
		this.executorService = Executors.newFixedThreadPool(poolSize);
		
		for( int i = 0; i < poolSize; i++) executorService.execute( new WorkingThread<K,V>(this));	

		ScheduledExecutorService schedulerExecutorService = Executors.newSingleThreadScheduledExecutor();
		schedulerExecutorService.scheduleAtFixedRate( updateOffsets, this.duration, this.duration, TimeUnit.SECONDS);
	}
	
	
	public void close() {
		executorService.shutdown();
		logger.info("Thread Pool stoped");
	}
	
	
	protected void putPending(K key, ConsumerRecord<K,V> record) {
		this.pendings.put(key, record);
	}
	
	
	protected void removePending(K key) {
		this.pendings.remove(key);
	}	
	
	
	protected void setOffset( TopicPartition topicPartition, long offset) {	
		logger.info("TopicPartition: {} offset: {}", topicPartition, offset);
		this.offsetManager.put(topicPartition, offset);
	}
	
	
	protected Runnable updateOffsets = () -> {
		
		logger.info("Periodic Threaf");
		
		try {		
			Map<TopicPartition, OffsetAndMetadata> oldOffsets;
			Set<TopicPartition> topicPartitions = offsetManager.GetTopicPartitions();
			
			synchronized(this.consumer) { 
				oldOffsets = this.consumer.committed(topicPartitions);
			}
			
			Map<TopicPartition, OffsetAndMetadata> newOffsets = offsetManager.newOffsets(oldOffsets);		
			
			if ( !newOffsets.isEmpty() ) {
				synchronized(this.consumer) {
					this.consumer.commitSync(newOffsets);
				}
				offsetManager.moveOffsets(newOffsets);
			}
			
		} catch(Exception e) {
			logger.error("Exception: {}", e);
		}	
	};

}
