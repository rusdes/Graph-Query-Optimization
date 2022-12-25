package ldbc.group;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Decade;
import org.apache.flink.api.java.tuple.Pair;
import org.apache.flink.api.java.tuple.Ennead;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PersonGroupLanguage implements CoGroupFunction<Pair<Long, String>, Ennead<Long, String, String, String, String, String, String, String, String>,
	Decade<Long, String, String, String, String, String, String, String, String, String>> {

	@Override
	public void coGroup(
			Iterable<Pair<Long, String>> languages,
			Iterable<Ennead<Long, String, String, String, String, String, String, String, String>> person,
			Collector<Decade<Long, String, String, String, String, String, String, String, String, String>> personWithEmailAndLanguage)
			throws Exception{

		String language = "";
		Iterator<Pair<Long, String>> languageItr = languages.iterator();
		Iterator<Ennead<Long, String, String, String, String, String, String, String, String>> personItr = person.iterator();
		while(languageItr.hasNext())
			language += languageItr.next().f1 + ", ";
		

		Ennead<Long, String, String, String, String, String, String, String, String> prs = personItr.next();

		if(language.length() > 2)
			personWithEmailAndLanguage.collect(new Decade<Long, String, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, prs.f8, language.substring(0, language.length() - 2)));
		else
			personWithEmailAndLanguage.collect(new Decade<Long, String, String, String, String, String, String, String, String, String>(prs.f0, prs.f1, prs.f2, prs.f3, prs.f4, prs.f5, prs.f6, prs.f7, prs.f8, language));
		
	}
}
