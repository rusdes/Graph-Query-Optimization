package operators.datastructures.kdtree;

import org.javatuples.Pair;
import org.javatuples.Quartet;

// Efficient Approach to find the median of an array
// using Randomized QuickSelect Algorithm
public class QuickSelect {
    Object a, b;
    int dim;
    // Partition function
    private int Partition(Object arr[], int l, int r) {
        
        Object lst = arr[r];
        int i = l, j = l;
        while (j < r) {
            Pair<String[], String> p1 = (Pair<String[], String>)arr[j];
            Pair<String[], String> p2 = (Pair<String[], String>)lst;

            // if (arr[j] < lst) {
            if ((p1.getValue0())[this.dim].compareTo(p2.getValue0()[this.dim]) < 0){
                arr = swap(arr, i, j);
                i++;
            }
            j++;
        }
        arr = swap(arr, i, r);
        return i;
    }

    // Picks a random pivot
    private int randomPartition(Object arr[], int l, int r) {
        // int n = r - l + 1;
        // int pivot = (int) (Math.random() % n);
        // arr = swap(arr, l + pivot, r);
        return Partition(arr, l, r);
    }

    private int MedianUtilRecur(Object arr[], int l, int r, int k) {
        // if l < r
        if (l <= r) {
            // Find the partition index
            int partitionIndex = randomPartition(arr, l, r);

            // If partition index = k, then
            // we found the median of odd
            // number element in arr[]
            if (partitionIndex == k) {
                b = arr[partitionIndex];
                if (!a.equals(-1))
                    return Integer.MIN_VALUE;
            }

            // If index = k - 1, then we get
            // a & b as middle element of
            // arr[]
            else if (partitionIndex == k - 1) {
                a = arr[partitionIndex];
                if (!b.equals(-1))
                    return Integer.MIN_VALUE;
            }

            // If partitionIndex >= k then
            // find the index in first half
            // of the arr[]
            if (partitionIndex >= k)
                return MedianUtilRecur(arr, l, partitionIndex - 1, k);

            // If partitionIndex <= k then
            // find the index in second half
            // of the arr[]
            else
                return MedianUtilRecur(arr, partitionIndex + 1, r, k);
        }
        return Integer.MIN_VALUE;
    }

    private void MedianUtilIter(Object arr[], int l, int r, int k) {

        // if l < r
        while (l <= r) {
            // Find the partition index
            int partitionIndex = randomPartition(arr, l, r);

            // If partition index = k, then
            // we found the median of odd
            // number element in arr[]
            if (partitionIndex == k) {
                b = arr[partitionIndex];
                if (!a.equals(-1))
                    return;
            }

            // If index = k - 1, then we get
            // a & b as middle element of
            // arr[]
            else if (partitionIndex == k - 1) {
                a = arr[partitionIndex];
                if (!b.equals(-1))
                    return;
            }

            // If partitionIndex >= k then
            // find the index in first half
            // of the arr[]
            if (partitionIndex >= k)
                r = partitionIndex - 1;
                // return MedianUtil(arr, l, partitionIndex - 1, k);

            // If partitionIndex <= k then
            // find the index in second half
            // of the arr[]
            else
                l = partitionIndex + 1; 
                // return MedianUtil(arr, partitionIndex + 1, r, k);
        }
        return;
    }

    // Function to find Median
    public Quartet<Integer, Integer, Integer, Object> median(Object arr[], int left, int right, int dimension) {
        a = -1;
        b = -1;
        dim = dimension;

        MedianUtilIter(arr, left, right, left + (right - left) / 2);
        // If even size, pick one on left of median.

        Quartet<Integer, Integer, Integer, Object> ans = new Quartet<Integer, Integer, Integer, Object>(left, right,
                left + (right - left) / 2, b);

        return ans;
    }

    // swap Function
    private Object[] swap(Object[] arr, int i, int j) {
        Object temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
        return arr;
    }
}