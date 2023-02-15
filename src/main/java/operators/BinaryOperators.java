package operators;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import operators.helper.FlatJoinFunction;
import operators.helper.JoinFunction;
import operators.helper.Collector;
import operators.helper.MapFunction;

/*
*
* All binary operators are implemented here.
* 1. Join operator. Each join operation joins two paths on a common vertex. The difference between joinOnBeforeVertices and joinOnAfterVertices is about the concatenation order of this two paths;
* 2. Union operator.
* 3. Intersection operator.
*
* */
public class BinaryOperators {

	// Each list contains the vertex IDs and edge IDs of a selected path so far
	private List<List<Long>> pathsLeft;
	private List<List<Long>> pathsRight;

	// Get the input graph, current columnNumber and the vertex and edges IDs
	public BinaryOperators(
			List<List<Long>> pathsLeft,
			List<List<Long>> pathsRight) {
		this.pathsLeft = pathsLeft;
		this.pathsRight = pathsRight;
	}

	// Join on after vertices
	public List<List<Long>> joinOnAfterVertices(int firstCol, int secondCol) {
		List<List<Long>> joinedResults = this.pathsLeft.parallelStream().map(list -> {
			List<List<Long>> intermediateList = this.pathsRight.stream().map(listRight -> {
				if(list.get(firstCol) == listRight.get(secondCol)){
					list.addAll(listRight.subList(secondCol+1, listRight.size()));
				}
				return list;
			}).collect(Collectors.toList());

			
			return intermediateList;
		}).flatMap(s -> s.stream()).collect(Collectors.toList());

		return joinedResults;
	}

	private static class JoinOnAfterVertices
			implements JoinFunction<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>> {
		private int col;

		public JoinOnAfterVertices(int secondCol) {
			this.col = secondCol;
		}

		@Override
		public ArrayList<Long> join(ArrayList<Long> leftVertices,
				ArrayList<Long> rightVertices) throws Exception {
			rightVertices.remove(this.col);
			leftVertices.addAll(rightVertices);
			return leftVertices;
		}
	}

	// Join on left vertices
	// public List<List<Long>> joinOnBeforeVertices(int firstCol, int secondCol) {
	// 	KeySelectorForColumns SelectorFirst = new KeySelectorForColumns(firstCol);
	// 	KeySelectorForColumns SelectorSecond = new KeySelectorForColumns(secondCol);

	// 	List<List<Long>> joinedResults = this.pathsLeft
	// 			.join(this.pathsRight)
	// 			.where(SelectorFirst)
	// 			.equalTo(SelectorSecond)
	// 			.with(new JoinOnBeforeVertices(firstCol));
	// 	return joinedResults;
	// }

	private static class JoinOnBeforeVertices
			implements JoinFunction<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>> {

		private int col;

		public JoinOnBeforeVertices(int firstCol) {
			this.col = firstCol;
		}

		@Override
		public ArrayList<Long> join(ArrayList<Long> leftPaths,
				ArrayList<Long> rightPaths) throws Exception {
			leftPaths.remove(this.col);
			rightPaths.addAll(leftPaths);
			return rightPaths;
		}
	}

	// Union
	// public List<List<Long>> union() {
	// 	List<List<Long>> unitedResults = this.pathsLeft
	// 			.union(this.pathsRight)
	// 			.distinct();
	// 	return unitedResults;
	// }

	// Intersection
	// public List<List<Long>> intersection() {
	// 	List<List<Long>> intersectedResults = this.pathsLeft
	// 			.join(this.pathsRight)
	// 			.where(0)
	// 			.equalTo(0)
	// 			.with(new IntersectionResultsMerge());
	// 	return intersectedResults;
	// }

	private static class IntersectionResultsMerge
			implements FlatJoinFunction<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>> {
		@Override
		public void join(ArrayList<Long> leftPath, ArrayList<Long> rightPath,
				Collector<ArrayList<Long>> intersectedPath) throws Exception {
			if (leftPath.equals(rightPath))
				intersectedPath.collect(leftPath);
		}
	}

}
