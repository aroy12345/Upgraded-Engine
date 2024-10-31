package jobs;

import flame.FlameContext;
import flame.FlamePair;
import flame.FlamePairRDD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import tools.Hasher;

public class Combine_Scores {
    static double a = 0.4;
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        FlamePairRDD hashTable = flameContext.fromTable("pt-crawl", row -> {
            if (row.columns().contains("url") && row.columns().contains("page")) {
                if (row.get("url").contains("...")) {
                    return null;
                }
                return row.get("url");
                return row.get("url");
            } else {
                return null;
            }
        }).mapToPair(urls -> new FlamePair(Hasher.hash(urls), urls));
        FlamePairRDD pageRank = hashTable.join(flameContext.fromTable("pt-pageranks", row -> row.key() + "," + row.get("rank")).mapToPair(row -> {
            String key = row.split(",")[0];
            if (row.split(",").length < 2) {
                return null;
            }
            String value = String.valueOf(Math.log(1 + Double.parseDouble(row.split(",")[1])));
            return new FlamePair(key, value);
        })).flatMapToPair(row -> {
       
                return Collections.singleton(new FlamePair(row._1(), row._2().split(",")[0] + "," + row._2().split(",")[1]));
        });
        FlamePairRDD tf_idf = hashTable.join(flameContext.fromTable("pt-tfidf", row -> row.key() + "," + row.get(row.columns().iterator().next())).mapToPair(row -> {
            int index = row.indexOf(",");
            String key = row.substring(0, index);
            String value = row.substring(index + 1);
            return new FlamePair(key, value);
        }).flatMapToPair(row -> {
            String[] urls = row._2().split(";");
            ArrayList<FlamePair> pairs = new ArrayList<>();
            for (String url : urls) {
                String[] parts = url.split(",");
                String doc = parts[0];
                String tf_idf_score = parts[1];
                pairs.add(new FlamePair(doc, row._1()+ "," + tf_idf_score ));
            }
            return pairs;
        })).flatMapToPair(row -> List.of(new FlamePair(row._2().split(",")[0], row._2().split(",")[1])));

        tf_idf.join(pageRank).flatMapToPair(row -> {
            String[] parts = row._2().split(",");
            if(parts.length < 3) {
                return null;
            }
            String word = parts[0];
            double tf_idf_score = Double.parseDouble(parts[1]);

            double pageRank_score = Double.parseDouble(parts[2]);
            return Collections.singleton(new FlamePair(row._1(), word + "," + harmonic_mean(tf_idf_score, pageRank_score)));
        }).flatMapToPair(row -> {
            String[] parts = row._2().split(",");
            return Collections.singleton(new FlamePair(parts[0], row._1() + "," + parts[1]));
        }).foldByKey("", (accumulator, url_rank) -> {
            if (accumulator.isEmpty()) {
                return url_rank;
            }
            return accumulator + ";" + url_rank;

        }).saveAsTable("pt-combinedscores");

    }
    static double harmonic_mean(double a, double b) {
        return 2 * a * b / (a + b);
    }
}
