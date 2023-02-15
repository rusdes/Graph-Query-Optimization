

// import operators.datastructures.EdgeExtended;
// import operators.datastructures.GraphExtended;
// import operators.datastructures.VertexExtended;
// import org.apache.flink.api.common.functions.MapFunction;
// import org.apache.flink.api.java.List;
// import org.apache.flink.api.java.ExecutionEnvironment;
// import org.javatuples.*;
// // import org.apache.flink.api.java.tuple.Pair;
// // import org.apache.flink.api.java.tuple.Triplet;
// // import org.apache.flink.api.java.tuple.Quintet;
// import org.apache.flink.core.fs.FileSystem.WriteMode;
// import queryplan.querygraph.QueryEdge;
// import queryplan.querygraph.QueryGraph;
// import queryplan.querygraph.QueryVertex;
// import queryplan.RuleBasedOptimizer;

// import java.util.HashMap;
// import java.util.HashSet;

// @SuppressWarnings("serial")
// public class RuleBasedOptimizerTest {
// 	public static void main(String[] args) throws Exception {
		
// 		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
// 		//String dir = "C:/Users/s146508/Desktop/ubuntu/5kPerson/";
// 		String dir = args[0];
		
// 		List<Triplet<Long, String, String>> verticesFromFile = env.readCsvFile(dir + "vertices.csv")
// 				.fieldDelimiter("|")
// 				.types(Long.class, String.class, String.class);
		
// 		List<Quintet<Long, Long, Long, String, String>> edgesFromFile = env.readCsvFile(dir + "edges.csv")
// 				.fieldDelimiter("|")
// 				.types(Long.class, Long.class, Long.class, String.class, String.class);
		
// 		List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices 
// 			= verticesFromFile.map(new VerticesFromFileToList());
		
// 		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges
// 			= edgesFromFile.map(new EdgesFromFileToList());
		
// 		GraphExtended<Long, HashSet<String>, HashMap<String, String>, 
// 	      Long, String, HashMap<String, String>> graph = GraphExtended.fromList(vertices, edges, env);
				
// 		switch(args[1]) {
// 			case "0" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
// 				//WHERE l.length >= 150
// 				//RETURN n
// 				HashMap<String, Pair<String, String>> commentProps = new HashMap<>();
				
// 				//browserUsed=Chrome
// 				commentProps.put("length", new Pair<String, String>(">=", "150")); 
				
// 				QueryVertex a = new QueryVertex("post",  new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person",  new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex c = new QueryVertex("comment", commentProps, false);
// 				QueryVertex d = new QueryVertex("tag",  new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d};
// 				QueryEdge[] es = {ab, cb, cd};
				
// 				QueryGraph g = new QueryGraph(vs, es);

// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			} 
			
// 			case "1" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
// 				//WHERE n.lastName = 'Yang'
// 				//RETURN n

// 				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
// 				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
				
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person",  personProps, true);
// 				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d};
// 				QueryEdge[] es = {ab, cb, cd};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
			
// 			case "2" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) - [:hasTag] -> (k:Tag)
// 				//WHERE n.lastName = 'Yang' AND n.browserUsed = 'Safari'
// 				//RETURN n
				
// 				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
// 				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
// 				personProps.put("browserUsed", new Pair<String, String>("eq", "Safari"));
				
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person",  personProps, true);
// 				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cd = new QueryEdge(c, d, "hasTag", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d};
// 				QueryEdge[] es = {ab, cb, cd};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
			
// 			case "3" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
// 				//WHERE (n) - [:studyAt] -> (o:organisation) AND
// 				//		(n) - [:isLocatedIn] -> (p:place) AND
// 				//      l.length >= 150
// 				//RETURN n
// 				HashMap<String, Pair<String, String>> commentProps = new HashMap<>();
// 				commentProps.put("length", new Pair<String, String>(">=", "150")); 
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>() , false);
// 				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex c = new QueryVertex("comment", commentProps, false);
// 				QueryVertex d = new QueryVertex("organisation", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
// 				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d, e};
// 				QueryEdge[] es = {ab, cb, bd, be};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
				
// 			}
// 			case "4" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
// 				//WHERE (n) - [:studyAt] -> (o:organisation) AND
// 				//		(n) - [:isLocatedIn] -> (p:place) AND
// 				// 		n.lastName = 'Yang'
// 				//RETURN n
				
// 				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
// 				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
				
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person", personProps, true);
// 				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("organisation", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
// 				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d, e};
// 				QueryEdge[] es = {ab, cb, bd, be};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
				
// 			}
// 			case "5" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
// 				//WHERE (n) - [:studyAt] -> (o:organisation) AND
// 				//		(n) - [:isLocatedIn] -> (p:place) AND
// 				// 		o.type = 'company'
// 				//RETURN n
				
// 				HashMap<String, Pair<String, String>> orgProps = new HashMap<>();
// 				orgProps.put("type", new Pair<String, String>("eq", "company")); 
				
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("organisation", orgProps, false);
// 				QueryVertex e = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
// 				QueryEdge be = new QueryEdge(b, e, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d, e};
// 				QueryEdge[] es = {ab, cb, bd, be};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
				
// 			}
// 			case "6" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
// 				//WHERE (n) - [:studyAt] -> (o:organisation) AND
// 				//		(l) - [:hasTag] -> (t:tag) AND
// 				//		(l) - [:isLocatedIn] -> (p:place) AND 
// 				// 		l.length >= 175
// 				//RETURN n
				
// 				HashMap<String, Pair<String, String>> commentProps = new HashMap<>();
// 				commentProps.put("length", new Pair<String, String>(">=", "175")); 
				
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex c = new QueryVertex("comment", commentProps, false);
// 				QueryVertex d = new QueryVertex("organisation",new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex f = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				
// 				QueryVertex[] vs = {a, b, c, d, e, f};
// 				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
				
// 			}

// 			case "7" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
// 				//WHERE (n) - [:studyAt] -> (o:organisation) AND
// 				//		(l) - [:hasTag] -> (t:tag) AND
// 				//		(l) - [:isLocatedIn] -> (p:place) AND 
// 				// 		n.lastName = 'Yang'
// 				//RETURN n
				
// 				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
// 				personProps.put("lastName", new Pair<String, String>("eq", "Yang")); 
				
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person", personProps, true);
// 				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("organisation",new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex f = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				
// 				QueryVertex[] vs = {a, b, c, d, e, f};
// 				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
				
// 			}

// 			case "8" : {
// 				//MATCH (m:post) - [:hasCreator] -> (n:person) <- [:hasCreator] - (l:comment) 
// 				//WHERE (n) - [:studyAt] -> (o:organisation) AND
// 				//		(l) - [:hasTag] -> (t:tag) AND
// 				//		(l) - [:isLocatedIn] -> (p:place) AND 
// 				// 		o.type = 'company'
// 				//RETURN n
				
// 				HashMap<String, Pair<String, String>> orgProps = new HashMap<>();
// 				orgProps.put("type", new Pair<String, String>("eq", "company")); 
				
// 				QueryVertex a = new QueryVertex("post", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex b = new QueryVertex("person", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex c = new QueryVertex("comment", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("organisation",orgProps, false);
// 				QueryVertex e = new QueryVertex("tag", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex f = new QueryVertex("place", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cb = new QueryEdge(c, b, "hasCreator", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bd = new QueryEdge(b, d, "studyAt", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ce = new QueryEdge(c, e, "hasTag", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cf = new QueryEdge(c, f, "isLocatedIn", new HashMap<String, Pair<String, String>>());
				
				
// 				QueryVertex[] vs = {a, b, c, d, e, f};
// 				QueryEdge[] es = {ab, cb, bd, ce, cf};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
				
// 			}
			
// 			case "9" : {
// 				//MATCH (a:Protein) - [:Interacts] -> (b:Protein) - [:EncodedOn] -> (c:Gene) 
// 				//RETURN a
// 				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex c = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bc = new QueryEdge(b, c, "EncodedOn", new HashMap<String, Pair<String, String>>());
				
				
// 				QueryVertex[] vs = {a, b, c};
// 				QueryEdge[] es = {ab, bc};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
				
// 			}
// 			case "10": {
// 				//MATCH (a:Protein) - [:Interacts] -> (b:Protein) - [:Reference] -> (c:Article) - [:PublishedIn] -> (d:Journal) 
// 				//RETURN a
// 				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
// 				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bc = new QueryEdge(b, c, "Reference", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cd = new QueryEdge(c, d, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
				
// 				QueryVertex[] vs = {a, b, c, d};
// 				QueryEdge[] es = {ab, bc, cd};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
// 			case "11": {
// 				//MATCH (a:Protein) - [:Interacts] -> (b:Protein) - [:Reference] -> (c:Article) - [:PublishedIn] -> (d:Journal) 
// 				//RETURN a
// 				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
// 				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bc = new QueryEdge(b, c, "Reference", new HashMap<String, Pair<String, String>>());
// 				QueryEdge cd = new QueryEdge(c, d, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
				
// 				QueryVertex[] vs = {a, b, c, d};
// 				QueryEdge[] es = {ab, bc, cd};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
// 			case "12": {
// 				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
// 				//WHERE (a) - [:Reference] -> (c:Article) AND
// 				//		(a) - [:EncodedOn] -> (d:Gene)    AND
// 				//		(a) - [:HasKeyword] -> (e:Keyword) 
// 				//RETURN a
// 				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ae = new QueryEdge(a, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d, e};
// 				QueryEdge[] es = {ab, ac, ad, ae};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
// 			case "13": {
// 				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
// 				//WHERE (a) - [:Reference] -> (c:Article) AND
// 				//		(a) - [:EncodedOn] -> (d:Gene)    AND
// 				//		(a) - [:HasKeyword] -> (e:Keyword) AND
// 				//		(a) - [:PublishedIn] -> (f:Journal)
// 				//RETURN a
// 				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex f = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ae = new QueryEdge(a, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
// 				QueryEdge af = new QueryEdge(a, f, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d, e, f};
// 				QueryEdge[] es = {ab, ac, ad, ae, af};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
// 			case "14": {
// 				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
// 				//WHERE (a) - [:Reference] -> (c:Article) AND
// 				//		(a) - [:EncodedOn] -> (d:Gene)    AND
// 				//		(b) - [:HasKeyword] -> (e:Keyword) AND
// 				//		(b) - [:PublishedIn] -> (f:Journal)
// 				//RETURN a
// 				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex f = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
				
// 				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
// 				QueryEdge be = new QueryEdge(b, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bf = new QueryEdge(b, f, "PublishedIn", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d, e, f};
// 				QueryEdge[] es = {ab, ac, ad,  be, bf};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
			
// 			case "15": {
// 				//MATCH (a:Protein) - [:Interacts] -> (b:Protein)  
// 				//WHERE (a) - [:Reference] -> (c:Article) AND
// 				//		(a) - [:EncodedOn] -> (d:Gene)    AND
// 				//		(b) - [:HasKeyword] -> (e:Keyword) AND
// 				//		(b) - [:PublishedIn] -> (f:Journal)AND
// 				//      (b) - [:Interacts] -> (h:Protein)
// 				//RETURN a
// 				QueryVertex a = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), true);
// 				QueryVertex b = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex c = new QueryVertex("Article", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex d = new QueryVertex("Gene", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex e = new QueryVertex("Keyword", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex f = new QueryVertex("Journal", new HashMap<String, Pair<String, String>>(), false);
// 				QueryVertex h = new QueryVertex("Protein", new HashMap<String, Pair<String, String>>(), false);
// 				QueryEdge ab = new QueryEdge(a, b, "Interacts", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ac = new QueryEdge(a, c, "Reference", new HashMap<String, Pair<String, String>>());
// 				QueryEdge ad = new QueryEdge(a, d, "EncodedOn", new HashMap<String, Pair<String, String>>());
// 				QueryEdge be = new QueryEdge(b, e, "HasKeyword", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bf = new QueryEdge(b, f, "PublishedIn", new HashMap<String, Pair<String, String>>());
// 				QueryEdge bh = new QueryEdge(b, h, "Interacts", new HashMap<String, Pair<String, String>>());
				
// 				QueryVertex[] vs = {a, b, c, d, e, f, h};
// 				QueryEdge[] es = {ab, ac, ad,  be, bf, bh};
				
// 				QueryGraph g = new QueryGraph(vs, es);
				
// 				RuleBasedOptimizer pg = new RuleBasedOptimizer(g, graph);
// 				List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> res = pg.genQueryPlan();
// 				res.writeAsText(args[2], WriteMode.OVERWRITE);
// 				env.execute();
// 				break;
// 			}
// 		}
// 	}

// 	private static class VerticesFromFileToList implements MapFunction<Triplet<Long, String, String>, VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {

// 		@Override
// 		public VertexExtended<Long, HashSet<String>, HashMap<String, String>> map(
// 				Triplet<Long, String, String> vertexFromFile) throws Exception {
			
// 			VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex = new VertexExtended<Long, HashSet<String>, HashMap<String, String>>();
			
// 			vertex.setVertexId(vertexFromFile.f0);
			
// 			HashSet<String> labels = new HashSet<>();
// 			String[] labs = vertexFromFile.f1.substring(1, vertexFromFile.f1.length()- 1).split(",");
// 			for(String label: labs) {
// 				labels.add(label);
// 			}
// 			vertex.setLabels(labels);
// 			HashMap<String, String> properties = new HashMap<>();
// 			vertex.setProps(properties);
// 			return vertex;
			
// 		}
		
// 	}
// 	 //[^=]+=([^= ]*( |$))* 
	
// 	private static class EdgesFromFileToList implements MapFunction<Quintet<Long, Long, Long, String, String>, 
// 						EdgeExtended<Long, Long, String, HashMap<String, String>>> {

// 		@Override
// 		public EdgeExtended<Long, Long, String, HashMap<String, String>> map(
// 				Quintet<Long, Long, Long, String, String> edgeFromFile) throws Exception {
			
// 			EdgeExtended<Long, Long, String, HashMap<String, String>> edge = new EdgeExtended<Long, Long, String, HashMap<String, String>>();
			
// 			edge.setEdgeId(edgeFromFile.f0);
// 			edge.setSourceId(edgeFromFile.f1);
// 			edge.setTargetId(edgeFromFile.f2);
// 			edge.setLabel(edgeFromFile.f3);

// 			HashMap<String, String> properties = new HashMap<>();
// 			if(edgeFromFile.f4.length() > 2){
// 				String[] keyAndValue = edgeFromFile.f4.substring(1, edgeFromFile.f4.length() - 2).split("=");
// 				if(keyAndValue.length >= 2) {
// 					properties.put(keyAndValue[0], keyAndValue[1]);
// 				}
// 			}
// 			edge.setProps(properties);
			
// 			return edge;
			
// 		}
		
// 	}
// }