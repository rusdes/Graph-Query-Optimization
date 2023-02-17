import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;
import org.javatuples.*;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryVertex;
import queryplan.*;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class CustomOptimizerTest {

    public static void main(String[] args) throws Exception {

        String dir = "src/test/java/Dataset";
        List<Triplet<Long, String, String>> verticesFromFile = readVerticesLineByLine(Paths.get(dir, "vertices.csv"));
        List<Quintet<Long, Long, Long, String, String>> edgesFromFile = readEdgesLineByLine(
                Paths.get(dir, "edges.csv"));

        List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = verticesFromFile.stream()
                .map(elt -> VertexFromFileToDataSet(elt))
                .collect(Collectors.toList());

        List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges = edgesFromFile.stream()
                .map(elt -> EdgeFromFileToDataSet(elt))
                .collect(Collectors.toList());

        GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph = GraphExtended
                .fromList(vertices, edges);

        CustomOptimizer co = new CustomOptimizer(graph);
        co.preprocess();

        HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
        canelaCoxProps.put("Name", new Pair<String, String>("eq", "Canela Cox"));
        QueryVertex a = new QueryVertex("Artist", canelaCoxProps, false);
        QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
        QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

        QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
        QueryEdge bc = new QueryEdge(b, c, "Performed", new HashMap<String, Pair<String, String>>());

        QueryVertex[] vs = { a, b, c };
        QueryEdge[] es = { ab, bc };
        QueryGraph g = new QueryGraph(vs, es);
    }

    public static List<Triplet<Long, String, String>> readVerticesLineByLine(Path filePath) throws Exception {
        List<Triplet<Long, String, String>> list = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(filePath)) {
            try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
                    .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
                    .build()) {
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    Triplet<Long, String, String> holder = new Triplet<Long, String, String>(Long.parseLong(line[0]),
                            line[1], line[2]);
                    list.add(holder);
                }
            }
        }
        return list;
    }

    public static List<Quintet<Long, Long, Long, String, String>> readEdgesLineByLine(Path filePath) throws Exception {
        List<Quintet<Long, Long, Long, String, String>> list = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(filePath)) {
            try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
                    .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
                    .build()) {
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    Quintet<Long, Long, Long, String, String> holder = new Quintet<Long, Long, Long, String, String>(
                            Long.parseLong(line[0]), Long.parseLong(line[1]),
                            Long.parseLong(line[2]), line[3], line[4]);
                    list.add(holder);
                }
            }
        }
        return list;
    }

    public static VertexExtended<Long, HashSet<String>, HashMap<String, String>> VertexFromFileToDataSet(
            Triplet<Long, String, String> vertexFromFile) {
        VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex = new VertexExtended<Long, HashSet<String>, HashMap<String, String>>();
        vertex.setVertexId(vertexFromFile.getValue0());

        HashSet<String> labels = new HashSet<>();
        String[] labs = vertexFromFile.getValue1().split(",");
        for (String label : labs) {
            labels.add(label);
        }
        vertex.setLabels(labels);

        HashMap<String, String> properties = new HashMap<>();
        String[] props = vertexFromFile.getValue2().split(",");

        if (props.length > 1) {
            for (int i = 0; i < props.length - 1; i = i + 2) {
                properties.put(props[i], props[i + 1]);
            }
        }
        vertex.setProps(properties);

        return vertex;
    }

    public static EdgeExtended<Long, Long, String, HashMap<String, String>> EdgeFromFileToDataSet(
            Quintet<Long, Long, Long, String, String> edgeFromFile) {

        EdgeExtended<Long, Long, String, HashMap<String, String>> edge = new EdgeExtended<Long, Long, String, HashMap<String, String>>();

        edge.setEdgeId(edgeFromFile.getValue0());
        edge.setSourceId(edgeFromFile.getValue1());
        edge.setTargetId(edgeFromFile.getValue2());
        edge.setLabel(edgeFromFile.getValue3());

        HashMap<String, String> properties = new HashMap<>();
        String[] props = edgeFromFile.getValue4().split(",");
        if (props.length > 1) {
            for (int i = 0; i < props.length - 1; i = i + 2) {
                properties.put(props[i], props[i + 1]);
            }
        }
        edge.setProps(properties);

        return edge;

    }
}