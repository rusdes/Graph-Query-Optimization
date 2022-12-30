package ldbc.join;

import java.util.HashMap;
import java.util.HashSet;

// import org.apache.flink.api.common.functions.JoinFunction;
import operators.helper.JoinFunction;
import org.apache.flink.api.java.tuple.Pair;
import org.apache.flink.api.java.tuple.Quartet;


public class EdgeIdReplacerRight implements JoinFunction<Pair<Long, Long>, Quartet<Long, HashSet<String>, HashMap<String, String>, Long>, Pair<Long, Long>>{
	
	@Override
	public Pair<Long, Long> join(Pair<Long, Long> edge,
			Quartet<Long, HashSet<String>, HashMap<String, String>, Long> vertex)
					throws Exception {
		return new Pair<Long, Long>(edge.f0, vertex.f0);
	}
}
