package com.sharedConsumer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetManager extends ConcurrentHashMap<TopicPartition,List<Long>> {

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);
	
		
	public void put(TopicPartition topicPartition, Long offset) {
		
		List<Long> data;	
		if ( super.containsKey(topicPartition)  ) {
			data = super.get(topicPartition);
		} else {
			data = new ArrayList<>();	
			super.put(topicPartition,data);
		}
		data.add(offset);
	}
	
	
	public Set<TopicPartition> GetTopicPartitions() {

		return new HashSet<>(Collections.list(this.keys()));
	}
	
	
	private static void moveOffset(List<Long> offsets, long offset) {
		
		for( int i=offsets.size() -1; i >=0; i--) {
			if ( offsets.get(i) <= offset ) offsets.remove(i);
		};
	}
	
	
	private static Long getNewOffset( List<Long> offsets, Long previousOffset) {
		
		Long[] arr = offsets.toArray(new Long[offsets.size()]);
		Arrays.sort(arr);
	
		Long currentOffset = previousOffset;
		for( int i=0; i<arr.length; i++) {
			if ( arr[i] == currentOffset + 1 ) {
				currentOffset++;
			} else {
				break;
			}
		}
		return currentOffset;
	}
	
	
	public Map<TopicPartition,OffsetAndMetadata> newOffsets(Map<TopicPartition,OffsetAndMetadata> oldOffsets) {
		
		Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
		
		oldOffsets.forEach( (k,v) -> {				
			Long oldOffset =  ( v == null ) ? -1L : v.offset();	
			List<Long> offsets = this.get(k);
			Long newOffset = OffsetManager.getNewOffset( offsets, oldOffset);
			if ( oldOffset < newOffset) newOffsets.put( k, new OffsetAndMetadata(newOffset));			
			logger.info("Before: {}  Partition: {} - {}", newOffsets, k, v);
		});		
		return newOffsets;
	}
	
	
	public void moveOffsets(Map<TopicPartition,OffsetAndMetadata> offsets) {
		
		offsets.forEach((k,v) -> {	
			long newOffset = v.offset();
			List<Long> list = this.get(k);
			for(int i= list.size() -1 ; i>=0; i--) {
				if ( list.get(i) <= newOffset) list.remove(i);
			}
			logger.info("After: {} Partition: {} - {}", newOffset, k, list);
		});	
	}
	
	
	public void close() {
		
	}
	 

	
	public static void main(String[] args) {
		
		OffsetManager mgr = new OffsetManager();
		mgr.put(new TopicPartition("quickstart-events",0), 1L);
		mgr.put(new TopicPartition("quickstart-events",2), 2L);
		mgr.put(new TopicPartition("quickstart-events",0), 5L);
		mgr.put(new TopicPartition("quickstart-events",0), 2L);
		mgr.put(new TopicPartition("quickstart-events",0), 6L);
		mgr.put(new TopicPartition("quickstart-events",0), 3L);
		
		System.out.println(mgr);
		
		List<Long> partionOffsets = mgr.get(new TopicPartition("quickstart-events",0) );
	
		
		Long newOffset = OffsetManager.getNewOffset(partionOffsets, 0L);
		System.out.println( newOffset);
		
		System.out.println(mgr);
	}

}
