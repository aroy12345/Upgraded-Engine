package jobs;

import flame.FlameContext;
import flame.FlamePair;
import flame.FlamePairRDD;

import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import tools.Hasher;

public class TF_IDF {
    static double a = 0.4;
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        int N = flameContext.fromTable("pt-crawl", row -> "1").count();
        FlamePairRDD idf =
                flameContext.fromTable("pt-index", row -> {
                    System.out.println(row.get(row.columns().iterator().next()));
                    int n_i = row.get(row.columns().iterator().next()).split(",").length;
                    return row.key() + "," + Math.log((double) N / n_i);
                }).mapToPair(row -> {
                    String[] parts = row.split(",");
                    return new FlamePair(parts[0], parts[1]);
                });

        FlamePairRDD tf = flameContext.fromTable("pt-crawl", row ->row.columns().contains("page") && row.columns().contains("url")? Hasher.hash(row.get("url")) + "," + row.get("page"):null).mapToPair(row -> {
            int index = row.indexOf(",");
            String key = row.substring(0, index);
            String value = row.substring(index + 1);
            return new FlamePair(key, value);
        }).flatMapToPair(row ->{
            String page = row._2();
            HashMap<String, Integer> wordCount = new HashMap<>();
            for (String word : page.split(" ")) {
                wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
            }
            int max = wordCount.values().stream().max(Integer::compareTo).orElse(0);
            HashMap<String, Double> wordFreq = new HashMap<>();
            for (String word : wordCount.keySet()) {
                wordFreq.put(word, (a + (1 -a) * wordCount.get(word) / max));
            }

            return wordFreq.entrySet().stream().map(entry -> new FlamePair(entry.getKey(),row._1() + "," + entry.getValue().toString())).collect(Collectors.toList());
        }).foldByKey("", (accumulator, url_tf) -> {
            if (accumulator.isEmpty()) {
                return url_tf;
            }
            return accumulator + ";" + url_tf;
        });
        idf.join(tf).flatMapToPair(row -> {
            System.out.println(row._2() + "row2");
            int index = row._2().indexOf(",");
            double idfValue = Double.parseDouble(row._2().substring(0, index));
            String[] tfValues = row._2().substring(index+1).split(";");
            StringBuilder value = new StringBuilder();
            for (String tfValue : tfValues) {
                String[] tfParts = tfValue.split(",");

                value.append(tfParts[0]).append(",").append(Math.log(1 + Double.parseDouble(tfParts[1]) * idfValue)).append(";");
            }
            return Collections.singleton(new FlamePair(row._1(), value.substring(0, value.length() - 1)));
        }).saveAsTable("pt-tfidf");
    }

}
