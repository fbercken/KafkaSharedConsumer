package com.sharedConsumer;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;


public class DeadLetterProducer<K,V> extends KafkaProducer<K,V> {

	private String topic;	
	
	private DeadLetterProducer(Builder<K,V> builder) {
		
		super(builder.getProps());
		this.topic = builder.topic;	
	}
	
	
	
	public void send(ConsumerRecord<K,V> consumerRecord) {
		
		 ProducerRecord<K,V> producerRecord = new ProducerRecord<>(topic, consumerRecord.key(), consumerRecord.value()); 
	     this.send(producerRecord);
	     this.flush();
	}
	
	
	
	
	public static class Builder<K,V> {
		
		private String topic;
		private String keySerializer;
		private String valueSerializer;
		private String bootstrapServer = "localhost:9092";
		
		
		private Properties getProps() {
			
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
			
			return props;
		}
		
		
		public Builder<K,V> setTopic(String topic) {
			this.topic = topic;
			return this;
		}
			
		public Builder<K,V> setKeySerializer(String keySerializer) {
			this.keySerializer = keySerializer;
			return this;
		}
		
		public Builder<K,V> setValueSerializer(String valueSerializer) {
			this.valueSerializer = valueSerializer;
			return this;
		}
		
		public Builder<K,V> setBootstrapServer(String bootstrapServer) {
			this.bootstrapServer = bootstrapServer;
			return this;
		}
		
		public DeadLetterProducer<K,V> build() {
			return new DeadLetterProducer<K,V>(this);
		}
	}
	
}
