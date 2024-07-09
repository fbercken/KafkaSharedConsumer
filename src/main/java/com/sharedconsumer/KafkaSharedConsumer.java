package com.sharedconsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaSharedConsumer implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaSharedConsumer.class);

	private int threads;
	private int duration;
	private String topic;
	private KafkaConsumer<String,String> consumer;	
	private MesssageWriterPool<String,String> writer;
	
	
	public KafkaSharedConsumer(String topic, int threads, int duration) {
		
		this.topic = topic;
		this.threads = threads;
		this.duration = duration;
		this.consumer = GetKafkaConsumer();
		this.consumer.subscribe( Collections.singletonList(this.topic));		
		this.writer = new MesssageWriterPool<String,String>( this.threads, this.duration, consumer);
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
		
		ConsumerRecord<String,String> record;
		Iterator<ConsumerRecord<String,String>> recordIterator;
		
		this.writer.start();
		
		while(true) {	
			synchronized(this.consumer) {
				recordIterator = this.consumer.poll(Duration.ofMillis(10)).iterator();
				
				while ( recordIterator.hasNext() ) {
					record = recordIterator.next();
					this.writer.add(record);
				}
			}
		}	
	}
	

	public static void main(String[] args) {
		
		String topic = ( args.length > 0 ) ? args[0] : "quickstart-events";
		int threads  = ( args.length > 1 ) ? Integer.valueOf(args[1]) : 3;
		int duration = ( args.length > 2 ) ? Integer.valueOf(args[2]) : 10;
		
		System.out.println(String.format("topic: %s  threads: %s Duration: %s ", topic, threads, duration));
		
		KafkaSharedConsumer consumer = new KafkaSharedConsumer(topic, threads, duration);		
		consumer.run();
	}

}
