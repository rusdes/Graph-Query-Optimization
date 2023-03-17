package operators;

import java.util.ArrayList;
import java.util.List;
import operators.helper.JoinFunction;

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
		for (int i_left = 0; i_left < this.pathsLeft.size(); i_left++){
			List<Long> list = this.pathsLeft.get(i_left);
			// List<List<Long>> joinedResults = this.pathsLeft.parallelStream().map(list -> {			
			for (int i_right = 0; i_right < this.pathsRight.size(); i_right++){
				List<Long> listRight = this.pathsRight.get(i_right);

				if(list.get(firstCol).equals(listRight.get(secondCol))){
					list.addAll(listRight.subList(secondCol+1, listRight.size()));
				}
				this.pathsLeft.set(i_left, list);
			}
		}
		return this.pathsLeft;
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
}
