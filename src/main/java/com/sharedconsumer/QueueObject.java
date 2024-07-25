package com.sharedConsumer;

public class QueueObject<V> {

	V value;
	int retries;
	int maxRetries = 3;
	long nextTime;
	
	public QueueObject( int retries, long nextTime, V value) {
		
		this.retries = retries;
		this.nextTime = nextTime;
		this.value = value;	
	}
	
	public QueueObject(V value) {
		
		this.retries = 0;
		this.value = value;	
	}
	
	public QueueObject<V> nextRetry() {
		
		this.retries++;
		this.nextTime += 1000L;
		return this;
	}
	
}
