package ldbc.join;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Pair;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class EdgeIdReplacerLeft implements JoinFunction<Pair<Long, Long>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, Pair<Long, Long>>{
	
	@Override
	public Pair<Long, Long> join(Pair<Long, Long> edge,
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> vertex)
					throws Exception {
		return new Pair<Long, Long>(vertex.f0, edge.f1);
	}
}
