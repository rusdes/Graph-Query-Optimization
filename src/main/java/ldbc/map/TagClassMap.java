package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Triplet;
import org.apache.flink.api.java.tuple.Quartet;

@SuppressWarnings("serial")
public class TagClassMap implements CrossFunction<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>, 
	Triplet<Long, String, String>, Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> {
	
	private String[] tagclassItems;
	private long newId = 1;
	public TagClassMap(String[] tagclassItems) { this.tagclassItems = tagclassItems; }

	@Override
	public Quartet<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Quartet<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Triplet<Long, String, String> tagclass) throws Exception {

		Quartet<Long, HashSet<String>, HashMap<String, String>, Long> tagclassWithOriginId = new Quartet<>();

		//set vertex id
		tagclassWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(tagclassItems[0]);
		tagclassWithOriginId.f1 = labels;
		
		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(tagclassItems[1], tagclass.f0.toString());
		properties.put(tagclassItems[2], tagclass.f1.toString());
		properties.put(tagclassItems[3], tagclass.f2.toString());
		tagclassWithOriginId.f2 = properties;
		
		tagclassWithOriginId.f3 = tagclass.f0;
		
		return tagclassWithOriginId;
	}
}
