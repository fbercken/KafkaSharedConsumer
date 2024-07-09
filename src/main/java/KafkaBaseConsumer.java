import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaBaseConsumer implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaBaseConsumer.class);

	private KafkaConsumer<String,String> consumer;	

	
	public KafkaBaseConsumer() {
		
		this.consumer = GetKafkaConsumer();
		this.consumer.subscribe(Arrays.asList("quickstart-events"));
	}
	
	
	public void run() {	
		
		ConsumerRecords<String,String> records;
		
		while(true) {
			
			records = this.consumer.poll(Duration.ofMillis(10));	
	
			for (ConsumerRecord<String,String> record : records) {				
				try {
					Thread.sleep(5000);
				    logger.info(record.value().toString());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public KafkaConsumer<String,String> GetKafkaConsumer() {
		
		Properties props = new Properties();
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		return new KafkaConsumer<String,String>(props);
	}
	
	
	public static void main(String[] args) {
		
		KafkaBaseConsumer consumer = new KafkaBaseConsumer();
		consumer.run();
	}

}
