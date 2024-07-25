package com.sharedConsumer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class WorkingThread<K,V> implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(WorkingThread.class);

	private boolean running = true;
	private MesssageWriterPool<K,V> manager;
	
	
	public WorkingThread(MesssageWriterPool<K,V> manager) {
		
		this.manager = manager;
	}

	
	@Override
	public void run() {
		
		K key;
		ConsumerRecord<K,V> record = null;
		QueueObject<ConsumerRecord<K,V>> queueRecord = null;
		
		while(running) {		
			try {
				queueRecord = this.manager.take();
				record = queueRecord.value;
				key = record.key();
				this.manager.putPending(key,record);

				Thread.sleep(2000);
				logger.info(record.toString());
				
				this.manager.removePending(key);
				this.manager.setOffset( new TopicPartition(record.topic(), record.partition()), record.offset());

			} catch (Exception e) {
				logger.error("exception: {}", e);
				
				if ( queueRecord.retries < queueRecord.maxRetries ) {
					this.manager.resend(queueRecord);
				} else {
					this.manager.sendDeadLetter(queueRecord.value);
				}
			}
		}
		
	}
	
}
