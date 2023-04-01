import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.javatuples.Pair;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryVertex;

class Query {
    Long queryId;
    String queryTag;
    QueryGraph queryGraph;

    public Long getQueryId() {
        return queryId;
    }

    public void setQueryId(Long queryId) {
        this.queryId = queryId;
    }

    public String getQueryTag() {
        return queryTag;
    }

    public void setQueryTag(String queryTag) {
        this.queryTag = queryTag;
    }

    public QueryGraph getQueryGraph() {
        return queryGraph;
    }

    public void setQueryGraph(QueryGraph queryGraph) {
        this.queryGraph = queryGraph;
    }
}

public class BatchQueryProcessing {

    public static HashMap<String, List<Query>> LoadQueries(Path path)
            throws Exception {

        HashMap<String, List<Query>> queries = new HashMap<>();
        List<Query> simple = new ArrayList<>();
        List<Query> medium = new ArrayList<>();
        List<Query> complex = new ArrayList<>();
        try (Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toString()), "utf-8"))) {
            try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
                    .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
                    .build()) {
                String[] line;
                List<QueryEdge> qe = new ArrayList<>();
                List<QueryVertex> qv = new ArrayList<>();
                int mediumCount = 0;
                int complexCount = 0;
                while ((line = csvReader.readNext()) != null) {
                    long Id = Long.parseLong(line[0]);

                    HashMap<String, Pair<String, String>> personProps = new HashMap<>();
                    String[] pprops = line[2].split(",");
                    if (pprops.length > 1) {
                        for (int i = 0; i < pprops.length - 1; i = i + 3) {
                            personProps.put(pprops[i], new Pair<String, String>(pprops[i + 1], pprops[i + 2]));
                        }
                    }
                    QueryVertex person = new QueryVertex(line[1], personProps, true);

                    HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
                    String[] mprops = line[4].split(",");
                    if (mprops.length > 1) {
                        for (int i = 0; i < mprops.length - 1; i = i + 3) {
                            movieProps.put(mprops[i], new Pair<String, String>(mprops[i + 1], mprops[i + 2]));
                        }
                    }
                    QueryVertex movie = new QueryVertex(line[3], movieProps, true);

                    String edgeLabel = line[5];
                    String tag = line[6];

                    QueryEdge pm = new QueryEdge(person, movie, edgeLabel, new HashMap<String, Pair<String, String>>());

                    if (tag.equals("simple")) {
                        qv.add(movie);
                        qv.add(person);
                        qe.add(pm);
                        Query q = new Query();
                        q.setQueryId(Id);
                        q.setQueryTag(tag);
                        QueryVertex[] x = new QueryVertex[qv.size()];
                        QueryEdge[] y = new QueryEdge[qe.size()];
                        for (int i = 0; i < qv.size(); i++) {
                            x[i] = qv.get(i);
                        }
                        for (int i = 0; i < qe.size(); i++) {
                            y[i] = qe.get(i);
                        }
                        q.setQueryGraph(new QueryGraph(x, y));
                        qv.clear();
                        qe.clear();
                        simple.add(q);

                    } else if (tag.equals("medium")) {
                        qv.add(movie);
                        qv.add(person);
                        qe.add(pm);
                        mediumCount++;

                        if (mediumCount % 3 == 0) {
                            Query q = new Query();
                            q.setQueryId(Id);
                            q.setQueryTag(tag);
                            QueryVertex[] x = new QueryVertex[qv.size()];
                            QueryEdge[] y = new QueryEdge[qe.size()];
                            for (int i = 0; i < qv.size(); i++) {
                                x[i] = qv.get(i);
                            }
                            for (int i = 0; i < qe.size(); i++) {
                                y[i] = qe.get(i);
                            }
                            q.setQueryGraph(new QueryGraph(x, y));
                            qv.clear();
                            qe.clear();
                            medium.add(q);
                        }

                    } else if (tag.equals("complex")) {
                        qv.add(movie);
                        qv.add(person);
                        qe.add(pm);
                        complexCount++;

                        if (complexCount % 6 == 0) {
                            Query q = new Query();
                            q.setQueryId(Id);
                            q.setQueryTag(tag);
                            QueryVertex[] x = new QueryVertex[qv.size()];
                            QueryEdge[] y = new QueryEdge[qe.size()];
                            for (int i = 0; i < qv.size(); i++) {
                                x[i] = qv.get(i);
                            }
                            for (int i = 0; i < qe.size(); i++) {
                                y[i] = qe.get(i);
                            }
                            q.setQueryGraph(new QueryGraph(x, y));
                            qv.clear();
                            qe.clear();
                            complex.add(q);
                        }

                    }
                }
            }
        }
        queries.put("simple", simple);
        queries.put("medium", medium);
        queries.put("complex", complex);
        return queries;
    }

    public static void main(String[] args) throws Exception {

        String dir = "src/test/java/Queries";
        HashMap<String, List<Query>> queries = LoadQueries(Paths.get(dir, "queries.csv"));
        System.out.println("Simple, Medium and Complex QueryGraph Buckets generated\n");
        System.out.println("Difficulty\tCount\n-----------------------");
        for (String difficulty : queries.keySet()) {
            System.out.println(difficulty + "\t\t" + queries.get(difficulty).size());
        }
        // TO DO - Pipline these queries into the execution engine
    }

}
