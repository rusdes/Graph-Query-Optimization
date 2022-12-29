package ldbc.join;

import java.util.HashMap;
import java.util.HashSet;

// import org.apache.flink.api.common.functions.JoinFunction;
import operators.helper.JoinFunction;
import org.apache.flink.api.java.tuple.Triplet;
import org.apache.flink.api.java.tuple.Quartet;

public class EdgeIdReplacerRightThreeElements implements JoinFunction<Triplet<Long, Long, String>, Quartet<Long, HashSet<String>, HashMap<String, String>, Long>, 
	Triplet<Long, Long, String>>{

	@Override
	public Triplet<Long, Long, String> join(Triplet<Long, Long, String> edge,
			Quartet<Long, HashSet<String>, HashMap<String, String>, Long> vertex)
					throws Exception {
		return new Triplet<Long, Long, String>(edge.f0, vertex.f0, edge.f2);
	}
}

