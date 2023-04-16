package operators.datastructures.kdtree;

/**
 * 
 * based on work by Simon Levy
 * http://www.cs.wlu.edu/~levy/software/kd/
 */

import java.io.Serializable;
import java.util.Stack;
import java.util.Vector;

// K-D Tree node class
class StackElement {
    public KDNode t;
    public int lev;

    public StackElement(KDNode node, int l) {
        this.t = node;
        this.lev = l;
    }
}

class KDNode implements Serializable {

    // these are seen by KDTree
    HPoint k;

    Object v;

    KDNode left, right;

    boolean deleted;

    // Method ins translated from 352.ins.c of Gonnet & Baeza-Yates
    protected static KDNode ins(HPoint key, Object val, KDNode t, int lev, int K) {
        if (t == null) {
            t = new KDNode(key, val);
        } else if (key.equals(t.k)) {
            // "re-insert"
            if (t.deleted) {
                t.deleted = false;
                t.v = val;
            }
        } else if (key.coord[lev].compareTo(t.k.coord[lev]) > 0) {
            t.right = ins(key, val, t.right, (lev + 1) % K, K);
        } else {
            t.left = ins(key, val, t.left, (lev + 1) % K, K);
        }
        return t;
    }

    protected static KDNode ins_iterative(HPoint key, Object val, KDNode t, int lev, int K) {
        KDNode newNode = new KDNode(key, val);
        KDNode x = t;
        KDNode y = null;

        while (x != null) {
            y = x;
            lev = (lev + 1) % K;
            if (key.coord[lev].compareTo(x.k.coord[lev]) > 0) {
                x = x.right;
            } else {
                x = x.left;
            }
        }

        if (y == null) {
            y = newNode;
        } else if (key.coord[lev].compareTo(y.k.coord[lev]) > 0) {
            y.right = newNode;
        } else {
            y.left = newNode;
        }

        return y;
    }

    // Method srch translated from 352.srch.c of Gonnet & Baeza-Yates
    protected static KDNode srch(HPoint key, KDNode t, int K) {

        for (int lev = 0; t != null; lev = (lev + 1) % K) {

            if (!t.deleted && key.equals(t.k)) {
                return t;
            } else if (key.coord[lev].compareTo(t.k.coord[lev]) > 0) {
                t = t.right;
            } else {
                t = t.left;
            }
        }

        return null;
    }

    // Method rsearch translated from 352.range.c of Gonnet & Baeza-Yates

    protected static void rsearch_iterative(HPoint lowk, HPoint uppk, KDNode t, int lev, int K, Vector<KDNode> v) {
        Stack<StackElement> stack = new Stack<StackElement>();
        stack.push(new StackElement(t, lev));

        while (!stack.isEmpty()) {
            StackElement s = stack.pop();
            KDNode curNode = s.t;
            int curLev = s.lev;

            if (curNode == null)
                continue;

            if (lowk.coord[curLev].compareTo(curNode.k.coord[curLev]) <= 0) {
                stack.push(new StackElement(curNode.left, (curLev + 1) % K));
            }
            int j;
            for (j = 0; j < K && lowk.coord[j].compareTo(curNode.k.coord[j]) <= 0
                    && uppk.coord[j].compareTo(curNode.k.coord[j]) >= 0; j++)
                ;
            if (j == K)
                v.add(curNode);
            if (uppk.coord[curLev].compareTo(curNode.k.coord[curLev]) > 0) {
                stack.push(new StackElement(curNode.right, (curLev + 1) % K));
            }
        }
    }

    protected static void rsearch(HPoint lowk, HPoint uppk, KDNode t, int lev, int K, Vector<KDNode> v) {
        if (t == null)
            return;
        if (lowk.coord[lev].compareTo(t.k.coord[lev]) <= 0) {
            rsearch(lowk, uppk, t.left, (lev + 1) % K, K, v);
        }
        int j;
        for (j = 0; j < K && lowk.coord[j].compareTo(t.k.coord[j]) <= 0
                && uppk.coord[j].compareTo(t.k.coord[j]) >= 0; j++)
            ;
        if (j == K)
            v.add(t);
        if (uppk.coord[lev].compareTo(t.k.coord[lev]) > 0) {
            rsearch(lowk, uppk, t.right, (lev + 1) % K, K, v);
        }
    }

    // constructor is used only by class; other methods are static
    public KDNode(HPoint key, Object val) {
        k = key;
        v = val;
        left = null;
        right = null;
        deleted = false;
    }

    protected String toString(int depth) {
        String s = k + "  " + v + (deleted ? "*" : "");
        if (left != null) {
            s = s + "\n" + pad(depth) + "L " + left.toString(depth + 1);
        }
        if (right != null) {
            s = s + "\n" + pad(depth) + "R " + right.toString(depth + 1);
        }
        return s;
    }

    private static String pad(int n) {
        String s = "";
        for (int i = 0; i < n; ++i) {
            s += " ";
        }
        return s;
    }
}