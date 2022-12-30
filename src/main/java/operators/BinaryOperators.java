package operators;

import java.util.ArrayList;
import java.util.List;

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
	private List<Long> pathsLeft;
	private List<Long> pathsRight;

	// Get the input graph, current columnNumber and the vertex and edges IDs
	public BinaryOperators(
			List<Long> pathsLeft,
			List<Long> pathsRight) {
		this.pathsLeft = pathsLeft;
		this.pathsRight = pathsRight;
	}

	// Join on after vertices
	public List<Long> joinOnAfterVertices(int firstCol, int secondCol) {
		KeySelectorForColumns SelectorFirst = new KeySelectorForColumns(firstCol);
		KeySelectorForColumns SelectorSecond = new KeySelectorForColumns(secondCol);

		List<Long> joinedResults = this.pathsLeft
				.join(this.pathsRight)
				.where(SelectorFirst)
				.equalTo(SelectorSecond)
				.with(new JoinOnAfterVertices(secondCol));
		return joinedResults;
	}

	private static class JoinOnAfterVertices implements JoinFunction<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>> {
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
	public List<Long> joinOnBeforeVertices(int firstCol, int secondCol) {
		KeySelectorForColumns SelectorFirst = new KeySelectorForColumns(firstCol);
		KeySelectorForColumns SelectorSecond = new KeySelectorForColumns(secondCol);

		List<Long> joinedResults = this.pathsLeft
				.join(this.pathsRight)
				.where(SelectorFirst)
				.equalTo(SelectorSecond)
				.with(new JoinOnBeforeVertices(firstCol));
		return joinedResults;
	}

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
	public List<Long> union() {
		List<Long> unitedResults = this.pathsLeft
				.union(this.pathsRight)
				.distinct();
		return unitedResults;
	}

	// Intersection
	public List<Long> intersection() {
		List<Long> intersectedResults = this.pathsLeft
				.join(this.pathsRight)
				.where(0)
				.equalTo(0)
				.with(new IntersectionResultsMerge());
		return intersectedResults;
	}

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
