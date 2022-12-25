package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Triplet;
import org.apache.flink.api.java.tuple.Quartet;

@SuppressWarnings("serial")
public class ForumMap implements CrossFunction<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>, 
	Triplet<Long, String, String>, Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> {
	
	private String[] forumItems;
	private long newId = 1;
	
	public ForumMap(String[] forumItems) {
		this.forumItems = forumItems;
	}

	@Override
	public Quartet<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Quartet<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Triplet<Long, String, String> forum) throws Exception {
		
		Quartet<Long, HashSet<String>, HashMap<String, String>, Long> forumWithOriginId = new Quartet<>();

		//set vertex id
		forumWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(forumItems[0]);
		forumWithOriginId.f1 = labels;
		

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(forumItems[1], forum.f0.toString());
		properties.put(forumItems[2], forum.f1.toString());
		properties.put(forumItems[3], forum.f2.toString());
		forumWithOriginId.f2 = properties;
		
		forumWithOriginId.f3 = forum.f0;
		
		return forumWithOriginId;
	}
}
