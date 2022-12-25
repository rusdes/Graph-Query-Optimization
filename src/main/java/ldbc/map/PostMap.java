package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Quartet;
import org.apache.flink.api.java.tuple.Octet;

@SuppressWarnings("serial")
public class PostMap implements CrossFunction<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>, 
	Octet<Long, String, String, String, String, String, String, String>, Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> {

	private String[] postItems;
	private long newId = 1;
	public PostMap(String[] postItems) { this.postItems = postItems; }

	@Override
	public Quartet<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Quartet<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Octet<Long, String, String, String, String, String, String, String> post) throws Exception {
		
		Quartet<Long, HashSet<String>, HashMap<String, String>, Long> postWithOriginId = new Quartet<>();
		
		//set id
		postWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(postItems[0]);
		postWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(postItems[1], post.f0.toString());
		properties.put(postItems[2], post.f1.toString());
		properties.put(postItems[3], post.f2.toString());
		properties.put(postItems[4], post.f3.toString());
		properties.put(postItems[5], post.f4.toString());
		properties.put(postItems[6], post.f5.toString());
		properties.put(postItems[7], post.f6.toString());
		properties.put(postItems[8], post.f7.toString());
		postWithOriginId.f2 = properties;
		
		postWithOriginId.f3 = post.f0;
		
		return postWithOriginId;
	}
}

