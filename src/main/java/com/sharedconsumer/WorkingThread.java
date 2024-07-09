package com.sharedconsumer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class WorkingThread<K,V> implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(WorkingThread.class);

	private MesssageWriterPool<K,V> manager;
	
	
	public WorkingThread(MesssageWriterPool<K,V> manager) {
		this.manager = manager;
		
		logger.info("Start workingThread");
	}

	
	@Override
	public void run() {
		
		K key;
		ConsumerRecord<K,V> record;
		
		while(true) {		
			try {
				record = this.manager.take();
				key = record.key();
				this.manager.putPending(key,record);
				try {
					Thread.sleep(2000);
					logger.info(record.toString());
					this.manager.removePending(key);
					this.manager.setOffset( new TopicPartition(record.topic(), record.partition()), record.offset());
				} catch(Exception e) {
					logger.error("exception: {}", e);
				} 
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
