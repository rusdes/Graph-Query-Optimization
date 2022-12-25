package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Quartet;
import org.apache.flink.api.java.tuple.Sextet;

@SuppressWarnings("serial")
public class CommentMap implements MapFunction<Sextet<Long, String, String, String, String, String>,
		Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> {

	private String[] commentItems;
	private long newId = 0;
	
	public CommentMap(String[] commentItems) {
		this.commentItems = commentItems;
	}

	@Override
	public Quartet<Long, HashSet<String>, HashMap<String, String>, Long>
 		map(Sextet<Long, String, String, String, String, String> comment)
 				throws Exception {

		Quartet<Long, HashSet<String>, HashMap<String, String>, Long> commentWithOriginId = new Quartet<>();

		//set vertex id

		commentWithOriginId.f0 = newId;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(commentItems[0]);
		commentWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(commentItems[1], comment.f0.toString());
		properties.put(commentItems[2], comment.f1.toString());
		properties.put(commentItems[3], comment.f2.toString());
		properties.put(commentItems[4], comment.f3.toString());
		properties.put(commentItems[5], comment.f4.toString());
		properties.put(commentItems[6], comment.f5.toString());
		commentWithOriginId.f2 = properties;
		
		commentWithOriginId.f3 = comment.f0;
		return commentWithOriginId;
	}
}
