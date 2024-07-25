package com.sharedConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaSharedConsumer implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaSharedConsumer.class);

	private boolean running;
	private KafkaConsumer<String,String> consumer;	
	private MesssageWriterPool<String,String> writer;
	
	
	public KafkaSharedConsumer(String topic, int size, int threads, int duration) {

		this.running = true;
		this.consumer = GetKafkaConsumer();
		this.consumer.subscribe( Collections.singletonList(topic));		
		
		this.writer = new MesssageWriterPool.Builder<String,String>()
			.setSize(size)	
			.setThhreads(threads)
			.setDuration(duration)
			.setKafkaConsumer(consumer)
			.setKafkaDeadletter(new DeadLetterProducer.Builder<String,String>()
				.setTopic("deadletters")
				.setKeySerializer("org.apache.kafka.common.serialization.StringSerializer")
				.setValueSerializer("org.apache.kafka.common.serialization.StringSerializer")
				.build()
			)	
			.build();
		
		this.writer.start();
	}
	
	
	
	public  KafkaConsumer<String,String> GetKafkaConsumer() {
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");	
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return new KafkaConsumer<String,String>(props);
	}
	
		
	public void run() {	
		
		while(running) {	
			synchronized(this.consumer) {
				
				ConsumerRecords<String,String> records = this.consumer.poll(Duration.ofMillis(10));				
				for( ConsumerRecord<String,String> record : records) {
					this.writer.add(new QueueObject<ConsumerRecord<String,String>>(record));
				}
			}
		}	
	}
	

	public static void main(String[] args) {
		
		String topic = ( args.length > 0 ) ? args[0] : "quickstart-events";
		int size = ( args.length > 1 ) ? Integer.valueOf(args[1]) : 1000;
		int threads  = ( args.length > 1 ) ? Integer.valueOf(args[1]) : 3;
		int duration = ( args.length > 2 ) ? Integer.valueOf(args[2]) : 10;
		
		System.out.println(String.format("topic: %s  threads: %s Duration: %s ", topic, threads, duration));
		
		KafkaSharedConsumer consumer = new KafkaSharedConsumer(topic, size, threads, duration);		
		consumer.run();
	}
	


}
