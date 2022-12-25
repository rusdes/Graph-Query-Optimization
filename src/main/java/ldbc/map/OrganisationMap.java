package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Quartet;

@SuppressWarnings("serial")
public class OrganisationMap implements CrossFunction<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>, 
		Quartet<Long, String, String, String>, Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> {
	
	private String[] organisationItems;
	private long newId = 1;
	public OrganisationMap(String[] organisationItems) { this.organisationItems = organisationItems; }

	@Override
	public Quartet<Long, HashSet<String>, HashMap<String, String>, Long> cross(
			Quartet<Long, HashSet<String>, HashMap<String, String>, Long> maxId,
			Quartet<Long, String, String, String> organisation) throws Exception {
		
		Quartet<Long, HashSet<String>, HashMap<String, String>, Long> organisationWithOriginId = new Quartet<>();

		//set vertex id
		organisationWithOriginId.f0 = newId + maxId.f0;
		this.newId ++;

		//set vertex labels
		HashSet<String> labels = new HashSet<>();
		labels.add(organisationItems[0]);
		organisationWithOriginId.f1 = labels;

		//set vertex properties
		HashMap<String, String> properties = new HashMap<>();
		properties.put(organisationItems[1], organisation.f0.toString());
		properties.put(organisationItems[2], organisation.f1.toString());
		properties.put(organisationItems[3], organisation.f2.toString());
		properties.put(organisationItems[4], organisation.f3.toString());
		organisationWithOriginId.f2 = properties;
		
		organisationWithOriginId.f3 = organisation.f0;
		
		return organisationWithOriginId;
	}
}
