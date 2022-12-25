package queryplan;

// import org.apache.commons.lang3.tuple.Pair;
// import org.apache.flink.api.java.List;

import org.javatuples.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
* Read the files storing statistical information and transform them to the vertices and edges defined in the package operators.datastructure
* */

public class StatisticsTransformation {
	String dir;
	// ExecutionEnvironment env;
	
	public StatisticsTransformation(String d) {
		dir = d;
		// env = e;
	}
	
	public HashMap<String, Pair<Long, Double>> getVerticesStatistics() throws Exception {
		List<Triplet<String, Long, Double>> vertices = env.readCsvFile(dir + "vertices")
				.fieldDelimiter("|")
				.types(String.class, Long.class, Double.class);
		
		ArrayList<Triplet<String, Long, Double>> vs = (ArrayList<Triplet<String, Long, Double>>) vertices.collect();
		HashMap<String, Pair<Long, Double>> vsi = new HashMap<>();
		for(Triplet<String, Long, Double> v: vs) {
			vsi.put(v.getValue0(), new Pair<Long, Double>(v.getValue1(), v.getValue2()));
		}
		return vsi;
	}


	public HashMap<String, Pair<Long, Double>> getEdgesStatistics() throws Exception {
		List<Triplet<String, Long, Double>> edges = env.readCsvFile(dir + "edges")
				.fieldDelimiter("|")
				.types(String.class, Long.class, Double.class);

		ArrayList<Triplet<String, Long, Double>> es = (ArrayList<Triplet<String, Long, Double>>) edges.collect();
		HashMap<String, Pair<Long, Double>> esi = new HashMap<>();
		for(Triplet<String, Long, Double> e: es) {
			esi.put(e.getValue0(), new Pair<Long, Double>(e.getValue1(), e.getValue2()));
		}
		return esi;
	}
}