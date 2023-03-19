package operators.helper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import operators.datastructures.GraphExtended;

public class print_result {
    GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;
    List<List<Long>> res1 = new ArrayList<>();
    List<List<Long>> res;
    String name_key;
    static String[][] table;

    public print_result(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> g,
    List<List<Long>> r,
    String key){
        graph= g;
        res= r;
        name_key= key;
        
        int number_of_rows= res.size()+1;
        int number_of_labels= res.get(0).size();
        String[] labels= new String[number_of_labels];

        table= new String[number_of_rows][number_of_labels];


        for(int i=0; i<number_of_labels; i++){
            String label= graph.getVertexByID(res.get(0).get(i)).getLabel();
            labels[i]=label;
        }

        table[0]= labels;

        for(int i=0; i<number_of_rows-1; i++){
            String[] row= new String[number_of_labels];
            for(int j=0; j<number_of_labels; j++){
                Long node= res.get(i).get(j);
                String value= graph.getVertexByID(node).getProps().get(name_key);
                String final_value = node.toString()+ ":" +value;
                row[j]=final_value;
            }
            table[i+1]=row;
        }
    }

    public void printTable() {
 
        boolean leftJustifiedRows = false;
    
        Map<Integer, Integer> columnLengths = new HashMap<>();
        Arrays.stream(table).forEach(a -> Stream.iterate(0, (i -> i < a.length), (i -> ++i)).forEach(i -> {
            if (columnLengths.get(i) == null) {
                columnLengths.put(i, 0);
            }
            if (columnLengths.get(i) < a[i].length()) {
                columnLengths.put(i, a[i].length());
            }
        }));

        final StringBuilder formatString = new StringBuilder("");
        String flag = leftJustifiedRows ? "-" : "";
        columnLengths.entrySet().stream().forEach(e -> formatString.append("| %" + flag + e.getValue() + "s "));
        formatString.append("|\n");
        System.out.println("");
     
        /*
         * Print table
         */
        Stream.iterate(0, (i -> i < table.length), (i -> ++i))
                .forEach(a -> System.out.printf(formatString.toString(), table[a]));
     
    }
    
}
