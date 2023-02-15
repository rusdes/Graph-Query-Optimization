package Dataset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import com.opencsv.CSVWriter;

public class convert_raw_dataset_yago {
    static int vertexID = 0;
    static int edgeID = 0;
    static HashMap<String, Integer> vertexIDMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        // initialize files if they dont exist
        writeData("src/test/java/Dataset/yago_edges.csv");
        writeData("src/test/java/Dataset/yago_vertices.csv");

        convertDataset(Paths.get("raw_datasets/yago/facts"));
    }

    public static void convertDataset(Path p) throws IOException {
        List<Path> files = Files.walk(p)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        int total = files.size();
        int count = 0;
        for (Path file : files) {
            File f = file.toFile();
            System.out.println(file.toString());
            readFile(f);
            // System.out.println(file.toString());
            System.out.println(count + " " + total);

            count += 1;
        }
        // Write vertices.csv
        writeData("src/test/java/Dataset/yago_vertices.csv");
    }

    public static void readFile(File f) throws FileNotFoundException {
        if (f.getName().equals("IsAExtractor.txt")) {
            return;
        }
        Scanner scan = new Scanner(f);
        ArrayList<String> v1 = new ArrayList<String>();
        ArrayList<String> v2 = new ArrayList<String>();

        String edge_label = f.getParent().toString()
                .substring(f.getParent().toString().lastIndexOf(File.separator) + 1);

        while (scan.hasNext()) {
            String curLine = scan.nextLine();
            String[] splitted = curLine.split("\t");
            if (splitted.length == 4) {
                String a = splitted[1].trim();
                String b = splitted[2].trim();

                ArrayList<String> candidate_nodes = new ArrayList();
                candidate_nodes.add(a);
                candidate_nodes.add(b);
                for (String candidate : candidate_nodes) {
                    if (!convert_raw_dataset_yago.vertexIDMap.containsKey(candidate)) {
                        convert_raw_dataset_yago.vertexIDMap.put(candidate, convert_raw_dataset_yago.vertexID);
                        convert_raw_dataset_yago.vertexID += 1;
                    }
                }
                v1.add(a);
                v2.add(b);
            }
        }
        scan.close();
        // Append to edges.csv
        writeData("src/test/java/Dataset/yago_edges.csv", v1, v2, edge_label);
    }

    public static void writeData(String filePath) {
        // Initialize and vertices
        writeData(filePath, new ArrayList<String>(), new ArrayList<String>(), "");
    }

    public static void writeData(String filePath, ArrayList<String> vertex_start, ArrayList<String> vertex_end,
            String edge_label) {

        // first create file object for file placed at location
        // specified by filepath
        File file = new File(filePath);

        try {
            // create FileWriter object with file as parameter
            Boolean file_exists = file.exists();
            FileWriter outputfile = new FileWriter(file, file_exists);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile, '|',
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            // create a List which contains String array
            List<String[]> data = new ArrayList<String[]>();
            if (!file_exists) {
                if (file.getName().equals("yago_edges.csv")) {
                    data.add(new String[] { "ID", "sourceID", "targetID", "Label", "Properties" });
                }
            } else {
                if (file.getName().equals("yago_edges.csv")) {
                    for (int i = 0; i < vertex_start.size(); i++) {
                        data.add(new String[] { Integer.toString(convert_raw_dataset_yago.edgeID),
                                Integer.toString(vertexIDMap.get(vertex_start.get(i))),
                                Integer.toString(vertexIDMap.get(vertex_end.get(i))),
                                edge_label,
                                "" });
                        convert_raw_dataset_yago.edgeID += 1;
                    }
                }
                if (file.getName().equals("yago_vertices.csv")) {
                    data.add(new String[] { "ID", "Labels", "Properties" });
                    for (String vertexName : vertexIDMap.keySet()) {
                        data.add(new String[] { Integer.toString(vertexIDMap.get(vertexName)), vertexName, "" });
                    }
                }
            }
            writer.writeAll(data);

            // closing writer connection
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
