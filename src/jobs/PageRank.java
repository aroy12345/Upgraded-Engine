package jobs;

import java.util.Arrays;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import flame.FlameContext;
import flame.FlamePair;
import flame.FlamePairRDD;
import tools.Hasher;
import tools.Logger;

public class PageRank {
    private static final Logger logger = Logger.getLogger(PageRank.class);
    private static final boolean DEBUG = false;
    private static final double decay = 0.85;

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Exactly 1 argument is expected, saw " + args.length);
        }
        double convergenceThreshold = Double.parseDouble(args[0]);
        if (DEBUG) {
            logger.error("Convergence threshold " + convergenceThreshold);
        }
        flameContext.setConcurrencyLevel(5);
        FlamePairRDD stateTable = flameContext.fromTable("pt-crawl", row -> {
            Set<String> columns = row.columns();
            if (!columns.contains("url") || !columns.contains("page")) {
                return null;
            }
            String url = row.get("url");
            URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
            List<String> urls = new ArrayList<>(List.of(url));
            if (row.columns().contains("urls"))
                urls.addAll(List.of(row.get("urls").split(",")));
            if (DEBUG) {
                logger.error(
                        "Extracting URLs for " + uri + " (" + Hasher.hash(uri.toString()) + "):");
                for (int i = 1; i < urls.size(); ++i) {
                    logger.error("  " + urls.get(i) + " --> " + Hasher.hash(urls.get(i)));
                }
            }
            return urls.stream().map(Hasher::hash).collect(Collectors.joining(","));
        }).mapToPair(urls -> {
            String[] parts = urls.split(",", 2);
            String url = parts[0];
            List<String> values = new ArrayList<>(List.of("1.0,1.0"));
            if (parts.length > 1) {
                values.add(parts[1]);
            }
            if (DEBUG) {
                logger.error("Creating initial state table: " + url + " --> "
                        + String.join(",", values));
            }
            return new FlamePair(url, String.join(",", values));
        });
        while (true) {
            stateTable = stateTable.flatMapToPair(pair -> {
                        if (DEBUG) {
                            logger.error("create out scores:");
                        }
                        String url = pair._1();
                        String[] parts = pair._2().split(",");
                        double rank = Double.parseDouble(parts[0]);
                        int numOutUrls = parts.length - 2;
                        List<FlamePair> outPairs = new ArrayList<>();
                        outPairs.add(new FlamePair(url, Double.toString(1 - decay)));
                        if (DEBUG) {
                            logger.error("  " + url + " <-- " + Double.toString(1 - decay));
                        }
                        for (int i = 2; i < parts.length; ++i) {
                            outPairs.add(
                                    new FlamePair(parts[i], Double.toString(decay * rank / numOutUrls)));
                            if (DEBUG) {
                                logger.error("  " + parts[i] + " <-- "
                                        + Double.toString(decay * rank / numOutUrls));
                            }
                        }
                        return outPairs;
                    }).foldByKey("0.0",
                            (accumulator, score) -> Double
                                    .toString(Double.parseDouble(accumulator) + Double.parseDouble(score)))
                    .join(stateTable).flatMapToPair(pair -> {
                        String url = pair._1();
                        String[] parts = pair._2().split(",", 4);
                        List<String> values = new ArrayList<>(List.of(parts[0], parts[1]));
                        if (parts.length > 3) {
                            values.add(parts[3]);
                        }
                        if (DEBUG) {
                            logger.error("Creating new state table: " + url + " --> "
                                    + String.join(",", values));
                        }
                        return List.of(new FlamePair(url, String.join(",", values)));
                    });
            int aboveThresholdCount = stateTable.flatMap(pair -> {
                String[] parts = pair._2().split(",");
                double score = Double.parseDouble(parts[0]);
                double prevScore = Double.parseDouble(parts[1]);
                return Math.abs(score - prevScore) >= convergenceThreshold ? List.of("") : List.of();
            }).count();
            if (aboveThresholdCount == 0) {
                break;
            }
        }
        stateTable.flatMapToPair(pair -> {
            if (DEBUG) {
                logger.error("Final write to KVS: '" + pair._1() + "'', '" + pair._2() + "'");
            }
            String[] parts = pair._2().split(",", 2);
            if (DEBUG) {
                logger.error("Final write to KVS: " + pair._1() + ", " + parts[0]);
            }
            flameContext.getKVS().put("pt-pageranks", pair._1(), "rank", parts[0]);
            return List.of();
        });
    }

}
