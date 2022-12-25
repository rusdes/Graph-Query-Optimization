package ldbc.group;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Pair;
import org.apache.flink.api.java.tuple.Octet;
import org.apache.flink.api.java.tuple.Ennead;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PersonGroupEmail implements CoGroupFunction<Pair<Long, String>, Octet<Long, String, String, String, String, String, String, String>,
		Ennead<Long, String, String, String, String, String, String, String, String>> {

	@Override
	public void coGroup(
			Iterable<Pair<Long, String>> emails,
			Iterable<Octet<Long, String, String, String, String, String, String, String>> person,
			Collector<Ennead<Long, String, String, String, String, String, String, String, String>> personWithEmail)
			throws Exception {
		
		String email = "";
		Iterator<Pair<Long, String>> emailItr = emails.iterator();
		Iterator<Octet<Long, String, String, String, String, String, String, String>> personItr = person.iterator();
		while(emailItr.hasNext()) {
			email += emailItr.next().f1 + ", ";
		}
		
		//email += emailItr.next().f1;
		
		Octet<Long, String, String, String, String, String, String, String> prs = personItr.next();
		
		if(email.length() > 0)
			personWithEmail.collect(new Ennead<Long, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, email.substring(0, email.length() - 2)));
		else
			personWithEmail.collect(new Ennead<Long, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, email));	
	}

}
