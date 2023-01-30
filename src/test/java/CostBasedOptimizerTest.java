// package test;

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

// @SuppressWarnings("serial")
public class CostBasedOptimizerTest {
	
	public static void main(String[] args) throws Exception {
		
		// ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		String dir = "src/test/java/Dataset";
		String testQuery = "0";
		// Path path = Paths.get(dir);
		List<Triplet<Long, String, String>> verticesFromFile = readVerticesLineByLine(Paths.get(dir, "vertices.csv"));
		List<Quintet<Long, Long, Long, String, String>> edgesFromFile = readEdgesLineByLine(Paths.get(dir, "edges.csv"));

		List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = verticesFromFile.stream()
																										.map(elt -> VertexFromFileToDataSet(elt))
																										.collect(Collectors.toList());

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges = edgesFromFile.stream()
																							 .map(elt -> EdgeFromFileToDataSet(elt))
																							 .collect(Collectors.toList());
		
		StatisticsTransformation sts = new StatisticsTransformation();
		HashMap<String, Pair<Long, Double>> vstat = sts.getVerticesStatistics();
		HashMap<String, Pair<Long, Double>> estat = sts.getEdgesStatistics();
		GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
	      Long, String, HashMap<String, String>> graph = GraphExtended.fromList(vertices, edges);
				
		switch(testQuery) {
			case "0" : {
				HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				canelaCoxProps.put("Name", new Pair<String, String>("eq", "Canela Cox"));
				QueryVertex a = new QueryVertex("Artist",  canelaCoxProps, false);
				QueryVertex b = new QueryVertex("Band",  new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Performed", new HashMap<String, Pair<String, String>>());

				QueryVertex[] vs = {a, b, c};
				QueryEdge[] es = {ab, bc};
				QueryGraph g = new QueryGraph(vs, es);
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				break;
			} 
			
			case "1" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
				//WHERE n.lastName = 'Yang'
				//RETURN n

				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex b = new QueryVertex("person",  personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d};
				QueryEdge[] es = {ab, cb, cd};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			
			case "2" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
				//WHERE n.lastName = 'Yang' AND n.browserUsed = 'Safari'
				//RETURN n
				
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
				personProps.put("browserUsed", new Pair<String, String>("eq", "Safari"));
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex b = new QueryVertex("person",  personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d};
				QueryEdge[] es = {ab, cb, cd};
				
				QueryGraph g = new QueryGraph(vs, es);

				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			
			case "3" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(n) - [:isLocatedIn] -> (p:place) AND
				//      l.length >= 150
				//RETURN n
				HashMap<String, Pair<String, String>> commentProps = new HashMap<>();
				commentProps.put("length", new Pair<String, String>(">=", "150")); 
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>() , false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", commentProps, false);
				QueryVertex d = new QueryVertex("organisation", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e};
				QueryEdge[] es = {ab, cb, bd, be};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
				
			}
			case "4" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(n) - [:isLocatedIn] -> (p:place) AND
				// 		n.lastName = 'Yang'
				//RETURN n
				
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e};
				QueryEdge[] es = {ab, cb, bd, be};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
				
			}
			case "5" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(n) - [:isLocatedIn] -> (p:place) AND
				// 		o.type = 'company'
				//RETURN n
				
				HashMap<String, Pair<String, String>> orgProps = new HashMap<>();
				orgProps.put("type", new Pair<String, String>("eq", "company")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation", orgProps, false);
				QueryVertex e = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e};
				QueryEdge[] es = {ab, cb, bd, be};
				
				QueryGraph g = new QueryGraph(vs, es);
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
				
			}
			case "6" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(l) - [:hasTag] -> (t:tag) AND
				//		(l) - [:isLocatedIn] -> (p:place) AND 
				// 		l.length >= 175
				//RETURN n
				
				HashMap<String, Pair<String, String>> commentProps = new HashMap<>();
				commentProps.put("length", new Pair<String, String>(">=", "175")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", commentProps, false);
				QueryVertex d = new QueryVertex("organisation",new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex f = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Pair<String, String>>());
				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
				
			}

			case "7" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(l) - [:hasTag] -> (t:tag) AND
				//		(l) - [:isLocatedIn] -> (p:place) AND 
				// 		n.lastName = 'Yang'
				//RETURN n
				
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", personProps, true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation",new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex f = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Pair<String, String>>());
				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
				
			}

			case "8" : {
				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
				//WHERE (n) - [:studyAt] -> (o:organisation) AND
				//		(l) - [:hasTag] -> (t:tag) AND
				//		(l) - [:isLocatedIn] -> (p:place) AND 
				// 		o.type = 'company'
				//RETURN n
				
				HashMap<String, Pair<String, String>> orgProps = new HashMap<>();
				orgProps.put("type", new Pair<String, String>("eq", "company")); 
				
				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("organisation",orgProps, false);
				QueryVertex e = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex f = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Pair<String, String>>());
				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
				
			}
			
			case "9" : {
				//MATCH (a:Protein) - [:Interacts] -> (b:Protein) - [:EncodedOn] -> (c:Gene) 
				//RETURN a
				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex c = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "EncodedOn", new HashMap<String, Pair<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c};
				QueryEdge[] es = {ab, bc};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			case "10": {
				//MATCH (a:Protein) - [:Interacts] -> (b:Protein) - [:Reference] -> (c:Article) - [:PublishedIn] -> (d:Journal) 
				//RETURN a
				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Reference", new HashMap<String, Pair<String, String>>());
				QueryEdge cd = new QueryEdge(c, d, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d};
				QueryEdge[] es = {ab, bc, cd};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			case "11": {
				//MATCH (a:Protein) - [:Interacts] -> (b:Protein) - [:Reference] -> (c:Article) - [:PublishedIn] -> (d:Journal) 
				//RETURN a
				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Reference", new HashMap<String, Pair<String, String>>());
				QueryEdge cd = new QueryEdge(c, d, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
				
				QueryVertex[] vs = {a, b, c, d};
				QueryEdge[] es = {ab, bc, cd};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			case "12": {
				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
				//WHERE (a) - [:Reference] -> (c:Article) AND
				//		(a) - [:EncodedOn] -> (d:Gene)    AND
				//		(a) - [:HasKeyword] -> (e:Keyword) 
				//RETURN a
				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
				QueryEdge ae = new QueryEdge(a, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e};
				QueryEdge[] es = {ab, ac, ad, ae};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			case "13": {
				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
				//WHERE (a) - [:Reference] -> (c:Article) AND
				//		(a) - [:EncodedOn] -> (d:Gene)    AND
				//		(a) - [:HasKeyword] -> (e:Keyword) AND
				//		(a) - [:PublishedIn] -> (f:Journal)
				//RETURN a
				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex f = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
				QueryEdge ae = new QueryEdge(a, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
				QueryEdge af = new QueryEdge(a, f, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, ac, ad, ae, af};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			case "14": {
				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
				//WHERE (a) - [:Reference] -> (c:Article) AND
				//		(a) - [:EncodedOn] -> (d:Gene)    AND
				//		(b) - [:HasKeyword] -> (e:Keyword) AND
				//		(b) - [:PublishedIn] -> (f:Journal)
				//RETURN a
				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex f = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
				
				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
				QueryEdge bf = new QueryEdge(b, f, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e, f};
				QueryEdge[] es = {ab, ac, ad,  be, bf};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}
			
			case "15": {
				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
				//WHERE (a) - [:Reference] -> (c:Article) AND
				//		(a) - [:EncodedOn] -> (d:Gene)    AND
				//		(b) - [:HasKeyword] -> (e:Keyword) AND
				//		(b) - [:PublishedIn] -> (f:Journal)AND
				//      (b) - [:Interacts] -> (h:Protein)
				//RETURN a
				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex f = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
				QueryVertex h = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
				QueryEdge be = new QueryEdge(b, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
				QueryEdge bf = new QueryEdge(b, f, "PublishedIn", new HashMap<String, Pair<String, String>>());
				QueryEdge bh = new QueryEdge(b, h, "Interacts", new HashMap<String, Pair<String, String>>());
				
				QueryVertex[] vs = {a, b, c, d, e, f, h};
				QueryEdge[] es = {ab, ac, ad,  be, bf, bh};
				
				QueryGraph g = new QueryGraph(vs, es);
				
				CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);
				List<HashSet<Long>> res = pg.generateQueryPlan();
				System.out.print(res);
				// res.writeAsText(args[2], WriteMode.OVERWRITE);
				// env.execute();
				break;
			}

		}
	}

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

	public static VertexExtended<Long, HashSet<String>, HashMap<String, String>> VertexFromFileToDataSet(Triplet<Long, String, String> vertexFromFile) {
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
			for (int i = 0; i < props.length-1; i=i+2) {
				properties.put(props[i], props[i+1]);
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
				for (int i = 0; i < props.length-1; i=i+2) {
					properties.put(props[i], props[i+1]);
				}
			}
			edge.setProps(properties);

			return edge;

		}
}