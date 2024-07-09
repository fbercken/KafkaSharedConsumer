import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.ArrayList;


public class ConcurrentMapList extends ConcurrentHashMap<String,List<Long>>{
	
		
	public synchronized void put(String key, Long value) {
		
		if ( !this.containsKey(key) ) {
			ArrayList<Long> arr = new ArrayList<Long>();
			arr.add(value);
			this.put(key, arr);
		} else {
			this.get(key).add(value);
		}
	}
	
	
	public Long getoffset(String key) {
		
		
		return null;
	}
	
	

	public static void main(String[] args) {

		
		ConcurrentMapList table = new ConcurrentMapList();
		
		table.put("a", 1l);
		table.put("a", 3l);
		System.out.println(table.get("a"));
		

	}

}
