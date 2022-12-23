package queryplan;

// import org.apache.flink.api.java.DataSet;
// import org.apache.flink.api.java.ExecutionEnvironment;
import org.javatuples.*;
// import org.apache.flink.api.java.tuple.Pair;
// import org.apache.flink.api.java.tuple.Triplet;

import com.opencsv.CSVReader;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
		List<Triplet<String, Long, Double>> vertices = readVerticesLineByLine(dir + "vertices");
		
		HashMap<String, Pair<Long, Double>> vsi = new HashMap<>();
		for(Triplet<String, Long, Double> v: vertices) {
			vsi.put(v.getValue0(), new Pair<Long, Double>(v.getValue1(), v.getValue2()));
		}
		return vsi;
	}


	public HashMap<String, Pair<Long, Double>> getEdgesStatistics() throws Exception {
		List<Triplet<String, Long, Double>> edges = readEdgesLineByLine(Paths.get(dir, "edges"));

		HashMap<String, Pair<Long, Double>> esi = new HashMap<>();
		for(Triplet<String, Long, Double> e: edges) {
			esi.put(e.getValue0(), new Pair<Long, Double>(e.getValue1(), e.getValue2()));
		}
		return esi;
	}

	public List<Triplet<Long, String, String>> readVerticesLineByLine(Path filePath) throws Exception {
		List<Triplet<Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReader(reader)) {
				String[] line;
				while ((line = csvReader.readNext()) != null) {
					Triplet<Long, String, String> holder = new Triplet<Long, String, String>(Long.parseLong(line[0]), line[1], line[2]);
					list.add(holder);
				}
			}
		}
		return list;
	}

	public static List<Quintet<Long, Long, Long, String, String>> readEdgesLineByLine(Path filePath) throws Exception {
		List<Quintet<Long, Long, Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReader(reader)) {
				String[] line;
				while ((line = csvReader.readNext()) != null) {
					Quintet<Long, Long, Long, String, String> holder = new Quintet<Long, Long, Long, String, String>(Long.parseLong(line[0]), Long.parseLong(line[1]),
																														Long.parseLong(line[2]), line[3], line[4]);
					list.add(holder);
				}
			}
		}
		return list;
	}
}
