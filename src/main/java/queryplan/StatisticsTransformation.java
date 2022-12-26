package queryplan;


import org.javatuples.*;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
* Read the files storing statistical information and transform them to the vertices and edges defined in the package operators.datastructure
* */

public class StatisticsTransformation {
	String dir = "src/test/java/Dataset_Statistics/";
	
	public HashMap<String, Pair<Long, Double>> getVerticesStatistics() throws Exception {
		List<Triplet<String, Long, Double>> vertices = readLineByLine(Paths.get(dir + "vertices.csv"));
		
		HashMap<String, Pair<Long, Double>> vsi = new HashMap<>();
		for(Triplet<String, Long, Double> v: vertices) {
			vsi.put(v.getValue0(), new Pair<Long, Double>(v.getValue1(), v.getValue2()));
		}
		return vsi;
	}

	public HashMap<String, Pair<Long, Double>> getEdgesStatistics() throws Exception {
		List<Triplet<String, Long, Double>> edges = readLineByLine(Paths.get(dir + "edges.csv"));

		HashMap<String, Pair<Long, Double>> esi = new HashMap<>();
		for(Triplet<String, Long, Double> e: edges) {
			esi.put(e.getValue0(), new Pair<Long, Double>(e.getValue1(), e.getValue2()));
		}
		return esi;
	}

	public static List<Triplet<String, Long, Double>> readLineByLine(Path filePath) throws Exception {
		List<Triplet<String, Long, Double>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader)
																//    .withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				while ((line = csvReader.readNext()) != null) {
					Triplet<String, Long, Double> holder = new Triplet<String, Long, Double> (line[0], Long.parseLong(line[1]), Double.parseDouble(line[2]));
					list.add(holder);
				}
			}
		}
		return list;
	}
}