package queryplan;

// import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.javatuples.*;
// import org.apache.flink.api.java.tuple.Pair;
// import org.apache.flink.api.java.tuple.Triplet;

import java.util.ArrayList;
import java.util.HashMap;

/*
* Read the files storing statistical information and transform them to the vertices and edges defined in the package operators.datastructure
* */

public class StatisticsTransformation {
	String dir;
	ExecutionEnvironment env;
	
	public StatisticsTransformation(String d, ExecutionEnvironment e) {
		dir = d;
		env = e;
	}
	
	public HashMap<String, Pair<Long, Double>> getVerticesStatistics() throws Exception {
		Triplet<String, Long, Double> vertices = env.readCsvFile(dir + "vertices")
				.fieldDelimiter("|")
				.types(String.class, Long.class, Double.class);
		
		ArrayList<Triplet<String, Long, Double>> vs = (ArrayList<Triplet<String, Long, Double>>) vertices.collect();
		HashMap<String, Pair<Long, Double>> vsi = new HashMap<>();
		for(Triplet<String, Long, Double> v: vs) {
			vsi.put(v.f0, new Pair<Long, Double>(v.f1, v.f2));
		}
		return vsi;
	}


	public HashMap<String, Pair<Long, Double>> getEdgesStatistics() throws Exception {
		Triplet<String, Long, Double> edges = env.readCsvFile(dir + "edges")
				.fieldDelimiter("|")
				.types(String.class, Long.class, Double.class);

		ArrayList<Triplet<String, Long, Double>> es = (ArrayList<Triplet<String, Long, Double>>) edges.collect();
		HashMap<String, Pair<Long, Double>> esi = new HashMap<>();
		for(Triplet<String, Long, Double> e: es) {
			esi.put(e.f0, new Pair<Long, Double>(e.f1, e.f2));
		}
		return esi;
	}
}
