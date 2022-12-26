package queryplan;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
// import org.apache.flink.api.common.functions.FlatMapFunction;
// import org.apache.flink.api.common.functions.MapFunction;
// import org.apache.flink.api.common.operators.Order;
// import org.apache.flink.api.java.List;
// import org.apache.flink.api.java.tuple.Pair;
// import org.apache.flink.api.java.tuple.Triplet;
// import org.apache.flink.api.java.tuple.Quintet;

import org.javatuples.*;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

// import org.apache.flink.api.java.ExecutionEnvironment;
// import org.apache.flink.util.Collector;
/**
 * Collect statistic information from graph database
 * Information includes the total number of edges and vertices,
 * vertex and edge labels and their corresponding proportion of the total number
 * */
// @SuppressWarnings("serial")
public class StatisticsCollector {
	public static void main(String[] args) throws Exception {
		
		// ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// env.setParallelism(1);
		String srcDir = "src/test/java/Dataset";
		String tarDir = "src/test/java/Dataset_Statistics";
		StatisticsCollector k = new StatisticsCollector(srcDir, tarDir);
		k.getEdgesStats();
		k.getVerticesStats();
	}	

	String srcDir;
	String tarDir;
	
	public StatisticsCollector(String s, String t) {
		srcDir = s;
		tarDir = t;
	}

	//This function will write a three-column file to the target directory. The three column consists of the vertex label names, the number of the vertices and the proportion.
	public List<Triplet<String, Long, Double>>  getVerticesStats () throws Exception {
		
		List<Triplet<Long, String, String>> verticesFromFile = readVerticesLineByLine(Paths.get(srcDir ,"vertices.csv"));
		List<Triplet<String, Long, Double>> vertices = ProportionComputationVertices(verticesFromFile); 
		
		writeDataForCustomSeparatorCSV(tarDir + "vertices", vertices);
		return vertices;
	}

	//This function will write a three-column file to the target directory. The three column consists of the edge label names, the number of the vertices and the proportion.
	public List<Triplet<String, Long, Double>> getEdgesStats () throws Exception {
		
		List<Quintet<Long, Long, Long, String, String>> edgesFromFile = readEdgesLineByLine(Paths.get(srcDir ,"edges.csv"));
		List<Triplet<String, Long, Double>> edges = ProportionComputationEdges(edgesFromFile); 
		
		writeDataForCustomSeparatorCSV(tarDir + "edges", edges);
		return edges;
	}
	
	private static List<Triplet<String, Long, Double>> ProportionComputationVertices(List<Triplet<Long, String, String>> data){
		long num = data.size();

		HashMap<String, Long> labels = new HashMap<>();
		
		for(Triplet<Long, String, String> d:data){
			String[] ls = d.getValue1().split(",");
			for(String label : ls) {
				if(labels.containsKey(label)){
					labels.put(label, labels.get(label) + 1);
				} else{
					labels.put(label, 1L);
				}
			}
		}
		List<Triplet<String, Long, Double>> retList = new ArrayList<>();
		retList.add(Triplet.with("vertices", num, 1.0));
		for (Map.Entry<String, Long> entry : labels.entrySet()) {
			Triplet<String, Long, Double> p = Triplet.with(entry.getKey(), entry.getValue(), (double)entry.getValue() * 1.0/num);
			retList.add(p);
		}
		// TODO: SORT THIS LIST IN DESCENDING ORDER
		return retList;
	}

	private static List<Triplet<String, Long, Double>> ProportionComputationEdges(List<Quintet<Long, Long, Long, String, String>> data){
		long num = data.size();

		HashMap<String, Long> labels = new HashMap<>();
		
		for(Quintet<Long, Long, Long, String, String> d:data){
			String[] ls = d.getValue3().split(",");
			for(String label : ls) {
				if(labels.containsKey(label)){
					labels.put(label, labels.get(label) + 1);
				} else{
					labels.put(label, 1L);
				}
			}
		}
		List<Triplet<String, Long, Double>> retList = new ArrayList<>();
		retList.add(Triplet.with("edges", num, 1.0));
		for (Map.Entry<String, Long> entry : labels.entrySet()) {
			Triplet<String, Long, Double> p = Triplet.with(entry.getKey(), entry.getValue(), (double)entry.getValue() * 1.0/num);
			retList.add(p);
		}
		// TODO: SORT THIS LIST IN DESCENDING ORDER
		return retList;
	}


	// Read Vertex from file
	public static List<Triplet<Long, String, String>> readVerticesLineByLine(Path filePath) throws Exception {
		List<Triplet<Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Triplet<Long, String, String> holder = new Triplet<Long, String, String> (Long.parseLong(line[0]), line[1], line[2]);
					list.add(holder);
				}
			}
		}
		return list;
	}

	public static List<Quintet<Long, Long, Long, String, String>> readEdgesLineByLine(Path filePath) throws Exception {
		List<Quintet<Long, Long, Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Quintet<Long, Long, Long, String, String> holder = new Quintet<Long, Long, Long, String, String> (Long.parseLong(line[0]), Long.parseLong(line[1]),
																													  Long.parseLong(line[2]), line[3], line[4]);
					list.add(holder);
				}
			}
		}
		return list;
	}

	public static void writeDataForCustomSeparatorCSV(String filePath, List<Triplet<String, Long, Double>> data){
		// first create file object for file placed at location
		// specified by filepath
		File file = new File(filePath);

		try {
			// create FileWriter object with file as parameter
			FileWriter outputfile = new FileWriter(file);

			// create CSVWriter with '|' as separator
			CSVWriter writer = new CSVWriter(outputfile, '|',
											CSVWriter.NO_QUOTE_CHARACTER,
											CSVWriter.DEFAULT_ESCAPE_CHARACTER,
											CSVWriter.DEFAULT_LINE_END);

			writer.writeAll(data);
			
			// closing writer connection
			writer.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}