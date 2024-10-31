package jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import flame.FlameContext;
import flame.FlamePair;
import flame.FlameRDDImpl;
import tools.Logger;
import tools.Hasher;
import flame.FlamePairRDDImpl;
import flame.FlameRDD;
import flame.FlamePairRDD;

public class Indexer {
    private static final Logger logger = Logger.getLogger(Indexer.class);
    private static final boolean DEBUG = false;

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        flameContext.setConcurrencyLevel(5);
        FlameRDD temp1 = flameContext.fromTable("pt-crawl", row -> {
            Set<String> columns = row.columns();
            if (!columns.contains("url") || !columns.contains("page")) {
                return null;
            }
            String url = Hasher.hash(row.get("url"));
            StringBuilder sb = new StringBuilder(url);
            for (String x : uniqueWords(row.get("page"))) {
            	sb.append(",").append(x);
            }
            return sb.toString();            
        });

        FlamePairRDD temp2 = temp1.flatMapToPair(urlAndWords -> {
            String[] parts = urlAndWords.split(",");
            String url = parts[0];
            List<FlamePair> pairs = new ArrayList<>();
            for (int i = 1; i < parts.length; i++) {
                pairs.add(new FlamePair(parts[i], url));
            }
            return pairs;
        });

        FlamePairRDD temp3 = temp2.foldByKey("", (accumulator, url) -> {
            StringBuilder sb = new StringBuilder();
            if (!accumulator.isEmpty()) {
                sb.append(accumulator).append(",");
            }
            sb.append(url);
            return sb.toString();
        });


        temp3.saveAsTable("pt-index");
    }

    private static List<String> uniqueWords(String page) {      
        return delimiterRegex.splitAsStream(page).filter(s -> !s.isBlank()).map(String::toLowerCase)
                .distinct().collect(Collectors.toList());
    }
    private static final Pattern delimiterRegex = Pattern.compile("[.,:;!?'\"()-]|\\s");
}
