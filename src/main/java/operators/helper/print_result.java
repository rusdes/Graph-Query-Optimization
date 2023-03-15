package operators.helper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import operators.datastructures.VertexExtended;
import operators.datastructures.GraphExtended;

public class print_result {
    GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;
    List<HashSet<HashSet<String>>> res1 = new ArrayList<>();
    List<List<List<String>>> res;
    int number_of_labels;
    int max_height;
    int a;
    int max_length=0;
    List<Integer> sizes = new ArrayList<>();
    String labels[];
    static String[][] table;
    String[] row;

    public print_result(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> g,
    List<HashSet<HashSet<String>>> r){
        graph= g;
        res= convert_to_list(r);
        number_of_labels= res.size();
        labels= new String[number_of_labels];
        
        for(int i=0; i<number_of_labels;i++){
            a= res.get(i).size();
            // System.out.println("help "+res.get(i).stream().findFirst().get().stream().findFirst().get());
            String key= res.get(i).stream().findFirst().get().stream().findFirst().get();
            long l=Long.parseLong(key);
            // String label= graph.getVertexByID(l).getLabel();
            // String label= res.get(i).stream().findFirst().get().getLabel();
            labels[i]= graph.getVertexByID(l).getLabel();
            sizes.add(a);
            if(a>=max_length){
                max_length=a;
            }
        }
        for(int i=0; i<number_of_labels; i++){
            if (sizes.get(i) < max_length) {
                fillZeros(res, i , max_length);
            } 
        }
        
        table= new String[max_length+1][number_of_labels];
        table[0]=labels;
            for(int i=1; i<=max_length; i++){
                row=new String[number_of_labels];
                String subrow;
                for(int j=0; j< number_of_labels; j++){
                    // System.out.println("res.get(j).get(i).get(0) "+res.get(j).get(i-1).get(0));
                    // System.out.println("res.get(j).get(i).get(1) "+ res.get(j).get(i-1).get(1));
                    subrow=res.get(j).get(i-1).get(0)+": "+ res.get(j).get(i-1).get(1);
                    row[j]= subrow;
                    // row[j]= "something";
                }
                table[i]= row;
            }
        }
    

    public void fillZeros(List<List<List<String>>> res, int i, int max_length) {
        List<String> zero= new ArrayList();
        zero.add("null");
        zero.add("null");
        for(int j = res.get(i).size(); j < max_length; j++) {
            res.get(i).add(zero);
        }
    }


    public List<List<List<String>>> convert_to_list(List<HashSet<HashSet<String>>> res1){
        List<List<List<String>>> res= new ArrayList<>();
        for (int i = 0; i < res1.size(); i++){
            List<List<String>> group= new ArrayList<>();
            for (HashSet<String> ele : res1.get(i)) {
                // Print HashSet data
                List<String> node= new ArrayList<>();
                for(String values: ele){
                    node.add(values);
                }
            group.add(node);
            }
            res.add(group);
        }
        return res;
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
        // System.out.println("columnLengths = " + columnLengths);
     
        /*
         * Prepare format String
         */
        final StringBuilder formatString = new StringBuilder("");
        String flag = leftJustifiedRows ? "-" : "";
        columnLengths.entrySet().stream().forEach(e -> formatString.append("| %" + flag + e.getValue() + "s "));
        formatString.append("|\n");
        // System.out.println("formatString = " + formatString.toString());
        System.out.println("");
     
        /*
         * Print table
         */
        Stream.iterate(0, (i -> i < table.length), (i -> ++i))
                .forEach(a -> System.out.printf(formatString.toString(), table[a]));
     
    }
    
}
