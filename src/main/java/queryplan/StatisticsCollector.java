package queryplan;


import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.*;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

/**
 * Collect statistic information from graph database
 * Information includes the total number of edges and vertices,
 * vertex and edge labels and their corresponding proportion of the total number
 * */
public class StatisticsCollector {
	
	String srcDir;
	String tarDir;
	
	public StatisticsCollector(String src, String tar) {
		srcDir = src;
		tarDir = tar;
	}
	public void collect() throws Exception {
		// String srcDir = "src/test/java/Dataset/compressed_imdb";
		// String tarDir = "src/test/java/Dataset/compressed_imdb/Dataset_Statistics";
		System.out.println("running stats collector");
		if(!Files.exists(Paths.get(this.tarDir + "/edges.csv"))){
			getEdgesStats();
			getVerticesStats();
		}
	}	

	public static class TupleFormat {
		private String label;
		private Long freq;
		private Double proportion;

		public TupleFormat(String label, Long freq, Double proportion){
			super();
			this.label = label;
			this.freq = freq;
			this.proportion = proportion;
		}

		@Override
		public String toString(){
		return this.label + "|" + String.valueOf(this.freq) + "|" + String.valueOf(this.proportion);
		}
	}

	//This function will write a three-column file to the target directory. The three column consists of the vertex label names, the number of the vertices and the proportion.
	public void getVerticesStats () throws Exception {
		List<Triplet<Long, String, String>> verticesFromFile = readVerticesLineByLine(Paths.get(srcDir ,"vertices.csv"));
		List<TupleFormat> vertices = ProportionComputationVertices(verticesFromFile); 
		
		writeDataForCustomSeparatorCSV(tarDir + "/vertices.csv", vertices);
		// return vertices;
	}

	//This function will write a three-column file to the target directory. The three column consists of the edge label names, the number of the vertices and the proportion.
	public void getEdgesStats () throws Exception {
		List<Quintet<Long, Long, Long, String, String>> edgesFromFile = readEdgesLineByLine(Paths.get(srcDir ,"edges.csv"));
		List<TupleFormat> edges = ProportionComputationEdges(edgesFromFile); 
		
		writeDataForCustomSeparatorCSV(tarDir + "/edges.csv", edges);
		// return edges;
	}
	
	private static List<TupleFormat> ProportionComputationVertices(List<Triplet<Long, String, String>> data){
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
		List<TupleFormat> retList = new ArrayList<>();
		retList.add(new TupleFormat("vertices", num, 1.0));
		for (Map.Entry<String, Long> entry : labels.entrySet()) {
			TupleFormat p = new TupleFormat(entry.getKey(), entry.getValue(), (double)entry.getValue() * 1.0/num);
			retList.add(p);
		}
		// TODO: SORT THIS LIST IN DESCENDING ORDER
		return retList;
	}

	private static List<TupleFormat> ProportionComputationEdges(List<Quintet<Long, Long, Long, String, String>> data){
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
		List<TupleFormat> retList = new ArrayList<>();
		retList.add(new TupleFormat("edges", num, 1.0));
		for (Map.Entry<String, Long> entry : labels.entrySet()) {
			TupleFormat p = new TupleFormat(entry.getKey(), entry.getValue(), (double)entry.getValue() * 1.0/num);
			retList.add(p);
		}
		// TODO: SORT THIS LIST IN DESCENDING ORDER
		return retList;
	}


	// Read Vertices from file
	public static List<Triplet<Long, String, String>> readVerticesLineByLine(Path filePath) throws Exception {
		List<Triplet<Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				while ((line = csvReader.readNext()) != null) {
					Triplet<Long, String, String> holder = new Triplet<Long, String, String> (Long.parseLong(line[0]), line[1], line[2]);
					list.add(holder);
				}
			}
		}
		return list;
	}

	// Read Edges from file
	public static List<Quintet<Long, Long, Long, String, String>> readEdgesLineByLine(Path filePath) throws Exception {
		List<Quintet<Long, Long, Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				while ((line = csvReader.readNext()) != null) {
					Quintet<Long, Long, Long, String, String> holder = new Quintet<Long, Long, Long, String, String> (Long.parseLong(line[0]), Long.parseLong(line[1]),
																													  Long.parseLong(line[2]), line[3], line[4]);
					list.add(holder);
				}
			}
		}
		return list;
	}

	public static void writeDataForCustomSeparatorCSV(String filePath, List<TupleFormat> data){
		// first create file object for file placed at location
		// specified by filepath
		try {
			// create FileWriter object with file as parameter
			PrintWriter writer = new PrintWriter(filePath);
			writer.println("Label|Frequency|Proportion");
			
			for(TupleFormat line : data){
				writer.println(line.toString());
			}
			// closing writer connection
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}