package ldbc.join;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Triplet;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class EdgeIdReplacerLeftThreeElements implements JoinFunction<Triplet<Long, Long, String>, Tuple4<Long, HashSet<String>, HashMap<String, String>, Long>, 
		Triplet<Long, Long, String>>{

	@Override
	public Triplet<Long, Long, String> join(Triplet<Long, Long, String> edge,
			Tuple4<Long, HashSet<String>, HashMap<String, String>, Long> vertex)
			throws Exception {
		return new Triplet<Long, Long, String>(vertex.f0, edge.f1, edge.f2);
	}
}
