package queryplan;

import operators.BinaryOperators;
import operators.ScanOperators;
import operators.UnaryOperators;
import operators.booleanExpressions.AND;
import operators.booleanExpressions.comparisons.LabelComparisonForEdges;
import operators.booleanExpressions.comparisons.LabelComparisonForVertices;
import operators.booleanExpressions.comparisons.PropertyFilterForEdges;
import operators.booleanExpressions.comparisons.PropertyFilterForVertices;
import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;
import operators.datastructures.kdtree.KDTree;
import operators.helper.FilterFunction;

import org.javatuples.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;

import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryGraphComponent;
import queryplan.querygraph.QueryVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static operators.datastructures.kdtree.Constants.STRING_MIN_VALUE;
import static operators.datastructures.kdtree.Constants.STRING_MAX_VALUE;

/*
* Cost-based graph query optimizer
* For filtering conditions, so far the optimizer can only process conjunctive filtering conditions
* */

public class CostBasedOptimzer {
	QueryGraph query;
	GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;
	HashMap<String, Pair<Long, Double>> verticesStats;
	HashMap<String, Pair<Long, Double>> edgesStats;
	String name_key;

	public CostBasedOptimzer(QueryGraph q,
			GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> g,
			HashMap<String, Pair<Long, Double>> vs,
			HashMap<String, Pair<Long, Double>> es,
			String name) {
		// Query should also be in the form of a graph
		query = q;
		graph = g;

		// statistics collected
		verticesStats = vs;
		edgesStats = es;

		name_key= name;
	}

	public void naiveMethodInitialComponent() {
		// Traverse each query vertex and generate a initial component
		for (QueryVertex qv : query.getQueryVertices()) {
			
			double est = verticesStats.get(qv.getLabel()).getValue1();
			ScanOperators s = new ScanOperators(graph);


			FilterFunction vf;
			FilterFunction newvf;
			vf = new LabelComparisonForVertices(qv.getLabel());

			if (!qv.getProps().isEmpty()) {
				HashMap<String, Pair<String, String>> props = qv.getProps();
				for (String k : props.keySet()) {
					newvf = new PropertyFilterForVertices(k, props.get(k).getValue0(), props.get(k).getValue1());
					vf = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>(vf, newvf);
				}
			}

			// List of singleton lists of initial vertices that satisfy the current filter
			// condition.
			// FilterFunction vf_exp = new LabelComparisonForVertices("Movie");
			// FilterFunction newvf_exp = new PropertyFilterForVertices("primaryTitle", "eq", "Carmencita");

			// vf_exp = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>(vf_exp, newvf_exp);


			// List<List<Long>> paths1 = s.getInitialVerticesByBooleanExpressions(new LabelComparisonForVertices("Movie"));
			// List<List<Long>> paths2 = s.getInitialVerticesByBooleanExpressions(newvf_exp);

			List<List<Long>> paths = s.getInitialVerticesByBooleanExpressions(vf);

			ArrayList<Object> cols = new ArrayList<>();
			cols.add(qv);
			qv.setComponent(new QueryGraphComponent(est, paths, cols));
		}
	}

	private Object[] queryKDTree(HashMap<String, Pair<String, String>> givenProp, String label, String type) {
		ArrayList<String> possibleKeys = graph.getPropKeySorted(label);

		String KDKeyMin[] = new String[possibleKeys.size()];
		String KDKeyMax[] = new String[possibleKeys.size()];

		if (type.equals("edge")) {
			KDKeyMin = new String[possibleKeys.size() + 1];
			KDKeyMax = new String[possibleKeys.size() + 1];

			KDKeyMin[0] = STRING_MIN_VALUE;
			KDKeyMax[0] = STRING_MAX_VALUE;
		}

		Set<String> givenPropKeys = givenProp.keySet();
		for (int i = 0; i < possibleKeys.size(); i += 1) {
			int curInd = i;
			if (type.equals("edge"))
				curInd += 1;

			if (givenPropKeys.contains(possibleKeys.get(i))) {
				// Complete based on operator
				Pair<String, String> pair = givenProp.get(possibleKeys.get(i));

				switch (pair.getValue0()) {
					case ">": {
						KDKeyMin[curInd] = pair.getValue1() + STRING_MIN_VALUE;
						KDKeyMax[curInd] = STRING_MAX_VALUE;
						break;
					}
					case "<": {
						KDKeyMin[curInd] = STRING_MIN_VALUE;
						KDKeyMax[curInd] = pair.getValue1().substring(0, pair.getValue1().length() - 1) + STRING_MIN_VALUE;
						break;
					}
					case ">=": {
						KDKeyMin[curInd] = pair.getValue1();
						KDKeyMax[curInd] = STRING_MAX_VALUE;
						break;
					}
					case "<=": {
						KDKeyMin[curInd] = STRING_MIN_VALUE;
						KDKeyMax[curInd] = pair.getValue1();
						break;
					}
					case "eq": // Fall through to "="
					case "=": {
						KDKeyMin[curInd] = pair.getValue1();
						KDKeyMax[curInd] = pair.getValue1();
						break;
					}
					case "<>": {
						// Not implemented
						break;
					}
				}
			} else {
				KDKeyMin[curInd] = STRING_MIN_VALUE;
				KDKeyMax[curInd] = STRING_MAX_VALUE;
			}
		}

		KDTree kdtree;
		if (type.equals("edge")) {
			kdtree = graph.getKDTreeEdgeByLabel(label);
		} else {
			kdtree = graph.getKDTreeVertexByLabel(label);
		}
		return kdtree.range(KDKeyMin, KDKeyMax);
	}

	public void KDTreeMethodInitialComponent() {
		// Query KD Tree from here to get inital vertex component
		// Traverse each query vertex and generate a initial component

		// TODO: Make parallel
		for (QueryVertex qv : query.getQueryVertices()) {
			double est = verticesStats.get(qv.getLabel()).getValue1();

			// Query KD Tree
			Object[] nodes = queryKDTree(qv.getProps(), qv.getLabel(), "vertex");

			List<List<Long>> paths = new ArrayList<>();
			for (Object node : nodes) {
				Long v_ind = ((VertexExtended<Long, HashSet<String>, HashMap<String, String>>) node).getVertexId();
				List<Long> list = Arrays.asList(v_ind);
				paths.add(list);
			}

			ArrayList<Object> cols = new ArrayList<>();
			cols.add(qv);
			qv.setComponent(new QueryGraphComponent(est, paths, cols));
		}
	}

	public List<HashSet<HashSet<String>>> generateQueryPlan(Set<String> options) throws Exception {
		// Naive or KD Tree method
		if (options.contains("vertex_naive")) {
			naiveMethodInitialComponent();
		} else if (options.contains("vertex_kdtree")) {
			KDTreeMethodInitialComponent();
		}

		ArrayList<QueryEdge> edges = new ArrayList<>(Arrays.asList(query.getQueryEdges()));
		while (!edges.isEmpty()) {
			// Traverse statistics, selects the edge with lowest cost
			double minEst = Double.MAX_VALUE;

			QueryEdge e = edges.get(0);
			for (QueryEdge cand : edges) {
				double estSrc = cand.getSourceVertex().getComponent().getEst();
				double estTar = cand.getTargetVertex().getComponent().getEst();
				double estEdge = edgesStats.get(cand.getLabel()).getValue0() * estSrc * estTar;
				if (minEst > estEdge) {
					minEst = estEdge;
					e = cand;
				}
			}
			edges.remove(e);

			List<List<Long>> paths = new ArrayList<>();
			List<List<Long>> joinedPaths;
			ArrayList<Object> leftColumns, rightColumns;

			// KDTree for labels as well

			Object[] filteredEdges = queryKDTree(e.getProps(), e.getLabel(), "edge");
			// System.out.println(filteredEdges);

			FilterFunction ef;
			FilterFunction newef;
			ef = new LabelComparisonForEdges(e.getLabel());
			if (!e.getProps().isEmpty()) {
				HashMap<String, Pair<String, String>> props = (HashMap<String, Pair<String, String>>) e.getProps()
						.clone();
				for (String k : props.keySet()) {
					newef = new PropertyFilterForEdges(k, props.get(k).getValue0(), props.get(k).getValue1());
					ef = new AND<EdgeExtended<Long, Long, String, HashMap<String, String>>>(ef, newef);
				}
			}

			// TODO: Check < or >
			if (e.getSourceVertex().getComponent().getEst() <= e.getTargetVertex().getComponent().getEst()) {
				int firstCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());
				int secondCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());

				List<List<Long>> curr_paths = e.getSourceVertex().getComponent().getData();

				FilterFunction vf;
				FilterFunction newvf;
				QueryVertex qv_out = e.getTargetVertex();
				vf = new LabelComparisonForVertices(qv_out.getLabel());
				if (!qv_out.getProps().isEmpty()) {
					HashMap<String, Pair<String, String>> props = qv_out.getProps();
					for (String k : props.keySet()) {
						newvf = new PropertyFilterForVertices(k, props.get(k).getValue0(), props.get(k).getValue1());
						vf = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>(vf, newvf);
					}
				}

				List<EdgeExtended<Long, Long, String, HashMap<String, String>>> filteredEdgesIntermed = new ArrayList<>();
				for (Object candEdge : filteredEdges) {
					Long IDTargetVertex = ((EdgeExtended<Long, Long, String, HashMap<String, String>>) candEdge)
							.getTargetId();
					VertexExtended<Long, HashSet<String>, HashMap<String, String>> candTarget = graph
							.getVertexByID(IDTargetVertex);
					if (vf.filter(candTarget)) {
						filteredEdgesIntermed.add((EdgeExtended<Long, Long, String, HashMap<String, String>>) candEdge);
					}
				}

				// efficient try
				// TODO: check with parallelStream()
				if (options.contains("edges_kdtree")) {
					paths = curr_paths.stream().map(list -> {
						List<List<Long>> intermediateList = new ArrayList<>();

						for (EdgeExtended<Long, Long, String, HashMap<String, String>> e1 : filteredEdgesIntermed) {
							if (e1.getSourceId() == list.get(firstCol)) {
								List<Long> cloned_list = new ArrayList<Long>(list);
								cloned_list.add(e1.getEdgeId());
								cloned_list.add(e1.getTargetId());
								intermediateList.add(cloned_list);
							}
						}
						return intermediateList;
					}).flatMap(s -> s.stream())
							.collect(Collectors.toList());

				} else if (options.contains("edges_naive")) {
					// Inefficient way
					UnaryOperators u = new UnaryOperators(graph, e.getSourceVertex().getComponent().getData());
					paths = u.selectOutEdgesByBooleanExpressions(firstCol, ef, vf);
				}

				leftColumns = e.getSourceVertex().getComponent().getColumns();

				BinaryOperators b = new BinaryOperators(paths, e.getTargetVertex().getComponent().getData());

				joinedPaths = b.joinOnAfterVertices(leftColumns.size() + 1, secondCol);

				rightColumns = (ArrayList<Object>) e.getTargetVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getTargetVertex());
			} else {
				int firstCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				int secondCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());

				List<List<Long>> curr_paths = e.getTargetVertex().getComponent().getData();

				FilterFunction vf;
				FilterFunction newvf;
				QueryVertex qv_out = e.getSourceVertex();
				vf = new LabelComparisonForVertices(qv_out.getLabel());
				if (!qv_out.getProps().isEmpty()) {
					HashMap<String, Pair<String, String>> props = qv_out.getProps();
					for (String k : props.keySet()) {
						newvf = new PropertyFilterForVertices(k, props.get(k).getValue0(), props.get(k).getValue1());
						vf = new AND<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>(vf, newvf);
					}
				}

				List<EdgeExtended<Long, Long, String, HashMap<String, String>>> filteredEdgesIntermed = new ArrayList<>();
				for (Object candEdge : filteredEdges) {
					Long IDSourceVertex = ((EdgeExtended<Long, Long, String, HashMap<String, String>>) candEdge)
							.getSourceId();
					VertexExtended<Long, HashSet<String>, HashMap<String, String>> candSource = graph
							.getVertexByID(IDSourceVertex);
					if (vf.filter(candSource)) {
						filteredEdgesIntermed.add((EdgeExtended<Long, Long, String, HashMap<String, String>>) candEdge);
					}
				}

				// efficient try
				// TODO: check with parallelStream()
				if (options.contains("edges_kdtree")) {
					paths = curr_paths.stream().map(list -> {
						List<List<Long>> intermediateList = new ArrayList<>();

						for (EdgeExtended<Long, Long, String, HashMap<String, String>> e1 : filteredEdgesIntermed) {
							if (e1.getTargetId() == list.get(firstCol)) {
								List<Long> cloned_list = new ArrayList<Long>(list);
								cloned_list.add(e1.getEdgeId());
								cloned_list.add(e1.getSourceId());
								intermediateList.add(cloned_list);
							}
						}
						return intermediateList;
					}).flatMap(s -> s.stream())
							.collect(Collectors.toList());

				} else if (options.contains("edges_naive")) {
					// Inefficient way
					UnaryOperators u = new UnaryOperators(graph, e.getTargetVertex().getComponent().getData());
					paths = u.selectInEdgesByBooleanExpressions(firstCol, ef, vf);
				}

				leftColumns = e.getTargetVertex().getComponent().getColumns();

				BinaryOperators b = new BinaryOperators(paths, e.getSourceVertex().getComponent().getData());
				joinedPaths = b.joinOnAfterVertices(leftColumns.size() + 1, secondCol);

				rightColumns = (ArrayList<Object>) e.getSourceVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getSourceVertex());
			}
			ArrayList<Object> columns = new ArrayList<>();
			columns.addAll(leftColumns);
			columns.add(e);
			columns.addAll(rightColumns);

			// TODO: Double Check and fix
			double est = minEst / verticesStats.get("vertices").getValue0();
			QueryGraphComponent gc = new QueryGraphComponent(est, joinedPaths, columns);

			for (Object o : columns) {
				if (o.getClass() == QueryVertex.class) {
					QueryVertex qv = (QueryVertex) o;
					qv.setComponent(gc);
				}
			}

		}

		// Where to collect outputs
		List<HashSet<HashSet<String>>> res = new ArrayList<>();
		// System.out.println("query.getQueryVertices() "+query.getQueryVertices() );
		for (QueryVertex qv : query.getQueryVertices()) {
			if (qv.isOutput()) {
				int pos = qv.getComponent().getVertexIndex(qv);
				// System.out.println("qv.getComponent() "+qv.getComponent());
				// System.out.println("pos "+pos);
				HashSet<HashSet<String>> store = new HashSet<>();
				// System.out.println("qv.getComponent().getData()"+ qv.getComponent().getData());
				for (List<Long> indices : qv.getComponent().getData()) {
					HashSet<String> vertex_value_set = new HashSet<>();
					Long key= indices.get(pos);
					String value= graph.getVertexByID(key).getProps().get(name_key);
					vertex_value_set.add(key.toString());
					vertex_value_set.add(value);
					store.add(vertex_value_set);
					// System.out.println("store "+store);
				}
				res.add(store);
			}
		}
		return res;
	}
}