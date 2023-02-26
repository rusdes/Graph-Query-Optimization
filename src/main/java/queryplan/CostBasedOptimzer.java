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

/*
* Cost-based graph query optimizer
* For filtering conditions, so far the optimizer can only process conjunctive filtering conditions
* */

public class CostBasedOptimzer {
	QueryGraph query;
	GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;
	HashMap<String, Pair<Long, Double>> verticesStats;
	HashMap<String, Pair<Long, Double>> edgesStats;

	public CostBasedOptimzer(QueryGraph q,
			GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> g,
			HashMap<String, Pair<Long, Double>> vs,
			HashMap<String, Pair<Long, Double>> es) {
		// Query should also be in the form of a graph
		query = q;
		graph = g;

		// statistics collected
		verticesStats = vs;
		edgesStats = es;
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
			List<List<Long>> paths = s.getInitialVerticesByBooleanExpressions(vf);

			ArrayList<Object> cols = new ArrayList<>();
			cols.add(qv);
			qv.setComponent(new QueryGraphComponent(est, paths, cols));
		}
	}

	public void KDTreeMethodInitialComponent(){
		// Query KD Tree from here to get inital vertex component
		//Traverse each query vertex and generate a initial component
		System.out.println("Using KD Tree");
		for(QueryVertex qv: query.getQueryVertices()){
			double est = verticesStats.get(qv.getLabel()).getValue1();

			// Query KD Tree
			ArrayList<String> possibleKeys = graph.getPropKeySorted(qv.getLabel());

			double KDKeyMin[] = new double[possibleKeys.size()];
			double KDKeyMax[] = new double[possibleKeys.size()];

			Set<String> givenPropKeys = qv.getProps().keySet();
			for(int i = 0; i < possibleKeys.size(); i += 1){
				if(givenPropKeys.contains(possibleKeys.get(i))){
					// Complete based on operator
					Pair<String, String> pair = qv.getProps().get(possibleKeys.get(i));
					switch (pair.getValue0()) {
						case ">": {
							// need to change key from hashcode to something else which keeps the order for comparision
						}
						case "<": {
							
						}
						case ">=": {
							
						}
						case "<=": {
							
						}
						case "=": {
							KDKeyMin[i] = pair.getValue1().hashCode();
							KDKeyMax[i] = pair.getValue1().hashCode();
						}
						case "<>": {

						}
					}
				}else{
					KDKeyMin[i] = Double.NEGATIVE_INFINITY;
					KDKeyMax[i] = Double.POSITIVE_INFINITY;
				}
			}

			List<List<Long>> paths = new ArrayList<>();

			KDTree kdtree = graph.getKDTreeByLabel(qv.getLabel());
			Object[] nodes = kdtree.range(KDKeyMin, KDKeyMax);
			for(Object node: nodes){
				Long v_ind = ((VertexExtended<Long, HashSet<String>, HashMap<String, String>>)node).getVertexId();
				List<Long> list = Arrays.asList(v_ind);
				paths.add(list);
			}

			ArrayList<Object> cols = new ArrayList<>();
			cols.add(qv);
			qv.setComponent(new QueryGraphComponent(est, paths, cols));
		}
	}

	public List<HashSet<Long>> generateQueryPlan() throws Exception {
		// Naive or KD Tree method
		String method = "kdtree";
		if (method.equals("naive")) {
			naiveMethodInitialComponent();
		} else if (method.equals("kdtree")) {
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

			List<List<Long>> paths, joinedPaths;
			ArrayList<Object> leftColumns, rightColumns;
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

			if (e.getSourceVertex().getComponent().getEst() <= e.getTargetVertex().getComponent().getEst()) {
				UnaryOperators u = new UnaryOperators(graph, e.getSourceVertex().getComponent().getData());
				int firstCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());
				int secondCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());

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

				paths = u.selectOutEdgesByBooleanExpressions(firstCol, ef, vf);

				leftColumns = e.getSourceVertex().getComponent().getColumns();

				BinaryOperators b = new BinaryOperators(paths, e.getTargetVertex().getComponent().getData());

				joinedPaths = b.joinOnAfterVertices(leftColumns.size() + 1, secondCol);

				rightColumns = (ArrayList<Object>) e.getTargetVertex().getComponent().getColumns().clone();
				rightColumns.remove(secondCol);
				rightColumns.add(0, e.getTargetVertex());
			} else {
				UnaryOperators u = new UnaryOperators(graph, e.getTargetVertex().getComponent().getData());
				int firstCol = e.getTargetVertex().getComponent().getVertexIndex(e.getTargetVertex());
				int secondCol = e.getSourceVertex().getComponent().getVertexIndex(e.getSourceVertex());

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

				paths = u.selectInEdgesByBooleanExpressions(firstCol, ef, vf);

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
		List<HashSet<Long>> res = new ArrayList<>();
		for (QueryVertex qv : query.getQueryVertices()) {
			if (qv.isOutput()) {
				int pos = qv.getComponent().getVertexIndex(qv);
				HashSet<Long> store = new HashSet<>();
				for (List<Long> indices : qv.getComponent().getData()) {
					store.add(indices.get(pos));
				}
				res.add(store);
				// UnaryOperators u = new UnaryOperators(graph, qv.getComponent().getData());
				// return u.projectDistinctVertices(qv.getComponent().getVertexIndex(qv));
			}
		}
		return res;
	}
}