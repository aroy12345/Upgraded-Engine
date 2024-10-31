package backend;

import flame.FlameContext;
import flame.FlameContextImpl;
import flame.FlamePair;
import flame.FlamePairRDD;
import kvs.KVSClient;
import objects.Website;

import java.util.*;

public class routes{ 

	 public static List<Website> search(String q, int resPage) throws Exception {
        System.out.println("Search called in backend");
        Map<String, Double> unsorted = Stream.of(q.split(" ")).flatMap(word -> {
            try {
                BufferedReader reader = new BufferedReader(new FileReader("pt-combinedscores/" + word));
                String all = reader.readLine();
                reader.close();
                String[] parts = all.split(" ");
                String value = parts[parts.length - 1];
                Map<String, Double> scores = new HashMap<>();
                for (String part : value.split(";")) {
                    String[] split = part.split(",");
                    scores.put(split[0], Double.parseDouble(split[1]));
                }
                return scores.entrySet().stream();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.summingDouble(Map.Entry::getValue)));
        Map<String, Double> sorted = unsorted.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        //get urls from desired range
        List<String> urls = new ArrayList<>(sorted.keySet());
        List<String> websites = urls.subList((resPage - 1) * 10, Math.min(resPage * 10, urls.size()));
        boolean hasNext = resPage * 10 < urls.size();
        Stream<Website> url_stream = websites.stream().map(url -> {
            try {
                System.out.println(Hasher.hash(url));
                BufferedReader reader = new BufferedReader(new FileReader("pt-crawl/" + Hasher.hash(url)));
                String all = reader.readLine();
                int title_index = all.indexOf("t̷̺̀î̸͌t̴̘̉l̸͂͌ė̵͛") + 20 + 1;
                String title_length = all.substring(title_index).substring(0, all.substring(title_index).indexOf(" "));
                int description_index = all.indexOf("d̶̅̈e̶̩̔s̴͚̾ċ̷̑r̵͆̈́i̵̝͐p̷͝tion") + 31 + 1;
                String description_length = all.substring(description_index).substring(0, all.substring(description_index).indexOf(" "));
                String title;
                if (Integer.parseInt(title_length) == 0) {
                    title = url;
                } else {
                    title = all.substring(title_index + title_length.length() + 1, title_index + title_length.length() + 1 + Integer.parseInt(title_length));
                }
                String description;
                if (Integer.parseInt(description_length) == 0) {
                    description = "No description available";
                } else {
                    description = all.substring(description_index + description_length.length() + 1, description_index + description_length.length() + 1 + Integer.parseInt(description_length));
                }
                if (description.contains("\\]")) description = description.substring(description.indexOf("\\]") + 2);
                return new Website(url, title, description);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return url_stream.toList();
    }
}

