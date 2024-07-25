package com.sharedConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MesssageWriterPool<K,V> extends LinkedBlockingDeque<QueueObject<ConsumerRecord<K,V>>>  {
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(MesssageWriterPool.class);

	private int threads;
	private int duration;
	private KafkaConsumer<K,V> consumer;
	private OffsetManager offsetManager;
	private ExecutorService executorService;
	private HashMap<K,ConsumerRecord<K,V>> pendings;

	private DeadLetterProducer<K,V> deadLetterProducer;
	private RetryManager<ConsumerRecord<K,V>> retryManager;

	
	private MesssageWriterPool( Builder<K,V> builder) {

		this.threads = builder.threads;
		this.duration = builder.duration;
		this.consumer = builder.consumer;
		this.deadLetterProducer = builder.deadletter;
		
		this.pendings = new HashMap<>();	
		this.offsetManager = new OffsetManager();
		this.retryManager = new RetryManager<ConsumerRecord<K,V>>(100);

	}
	
	public void start() {
		
		this.executorService = Executors.newFixedThreadPool(threads);
		
		for( int i = 0; i < threads; i++) executorService.execute( new WorkingThread<K,V>(this));	

		ScheduledExecutorService schedulerExecutorService = Executors.newSingleThreadScheduledExecutor();
		schedulerExecutorService.scheduleAtFixedRate( updateOffsets, this.duration, this.duration, TimeUnit.SECONDS);
	}
	
	
	public void close() {
		executorService.shutdown();
		
		this.offsetManager.close();
		this.retryManager.close();
		this.deadLetterProducer.close();	
		logger.info("Thread Pool stopped");
	}
	
	
	protected void putPending(K key, ConsumerRecord<K,V> record) {
		this.pendings.put(key, record);
	}
	
	
	protected void removePending(K key) {
		this.pendings.remove(key);
	}	
	
	
	public void sendDeadLetter(ConsumerRecord<K,V> record) {
		this.deadLetterProducer.send(record);
	}
	
	public void resend(QueueObject<ConsumerRecord<K,V>> queueObject) {
		this.retryManager.retry(queueObject);
	}
	
	
	protected void setOffset( TopicPartition topicPartition, long offset) {	
		logger.info("TopicPartition: {} offset: {}", topicPartition, offset);
		this.offsetManager.put(topicPartition, offset);
	}
	
	
	protected Runnable updateOffsets = () -> {
		
		try {
			logger.info("Offsets calculation");
			
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
			
			
			//retryManager.get()
			
		} catch(Exception e) {
			logger.error("Exception: {}", e);
		}	
	};
	
	
	
	
	public static class Builder<K,V> {
		
		private int size;
		private int threads;
		private int duration;
		private KafkaConsumer<K,V> consumer;
		private DeadLetterProducer<K,V> deadletter;
		
		
		public Builder<K,V> setSize(int size) {
			this.size = size;
			return this;
		}
		
		public Builder<K,V> setDuration(int duration) {
			this.duration = duration;
			return this;
		}
		
		public Builder<K,V> setThhreads(int threads) {
			this.threads = threads;
			return this;
		}
		
		public Builder<K,V> setKafkaConsumer(KafkaConsumer<K,V> consumer) {
			this.consumer = consumer;
			return this;
		}
		
		public Builder<K,V> setKafkaDeadletter(DeadLetterProducer<K,V> deadletter) {
			this.deadletter = deadletter;
			return this;
		}
		
		
		public MesssageWriterPool<K,V> build() {
			return new MesssageWriterPool<K,V>(this);
		}
	}
	
	
	

}
