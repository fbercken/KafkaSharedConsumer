package com.sharedConsumer;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryManager<V> extends PriorityBlockingQueue<QueueObject<V>> {
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(RetryManager.class);
	
	
	public RetryManager(int capacity)  {
		
		super(capacity,timeComparator);		
	}
	
	
	public void retry(QueueObject<V> queueObject) {
		
		this.add( queueObject.nextRetry() );
	}
	
	
	public void add( LinkedBlockingDeque<QueueObject<V>> queue) {	
		
		QueueObject<V> elt;
		
		while( !this.isEmpty() ) {
			
			if ( (elt = this.peek()) != null && elt.nextTime <= System.currentTimeMillis()) {
				queue.addFirst(elt);
				this.remove(elt);
			} else {
				break;
			} 
		}
	}
	
	
	public void close() {
		
	}
	
	
	public static Comparator<QueueObject<?>> timeComparator = new Comparator<QueueObject<?>>() {	
		@Override
		public int compare(QueueObject<?> o1, QueueObject<?> o2) {
			return (int) (o1.nextTime - o2.nextTime);
		}
	};

}
