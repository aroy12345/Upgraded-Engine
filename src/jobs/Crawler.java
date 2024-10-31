package jobs;

import flame.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.Hasher;
import tools.Serializer;
import tools.URLParser;

import java.util.stream.Collectors;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import static webserver.Server.*;

import kvs.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import webserver.Request;
import flame.FlameContext.RowToString;
import flame.FlamePairRDD.PairToPairIterable;
import flame.FlamePairRDD.PairToStringIterable;
import flame.FlameRDD.StringToIterable;
import flame.FlameRDD.StringToPairIterable;

import tools.Logger;

public class Crawler {
    private static List<Row> pushQueue = new ArrayList<>();

    private static final String LAST_ACCESSED_COL = "lastAccessedMs";
    private static final String USER_AGENT = "cis5550-crawler";
    private static final Logger logger = Logger.getLogger(Crawler.class);

    private static int maxDepth = 3;
    private static List<Pattern> blacklistPatterns = new ArrayList<>();

    private static void loadBlacklistPatterns(String filePath) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            for (String line : lines) {
                blacklistPatterns.add(convertPatternToRegex(line.trim()));
            }
        } catch (Exception e) {
            blacklistPatterns = new ArrayList<>();
            return;
        }
    }

    static String extractMetaDesc(byte[] b, String words) {
        String html = new String(b);
        if (html.contains("<meta name=\"description\" content=\"")) {
            int start = html.indexOf("<meta name=\"description\" content=\"");
            if (start == -1) {
                return words.substring(0,50);
            }
            start += 34;
            int end = html.indexOf("\"", start);
            return html.substring(start, end);
        }
        //implement fallback
        String temp =  words.substring(0,50);
        int strPos= words.indexOf("wikipedia");
        if(strPos != -1) {
            return temp.substring(0, strPos + "wikipedia".length());
        }
        else {
            return temp;
        }
    }
    static String extractTitle(byte[] b) {
        String html = new String(b);
        int start = html.indexOf("<title>");
        if (start == -1) {
            return "";
        }
        start += 7;
        int end = html.indexOf("</title>", start);
        return html.substring(start, end);
    }

    private static boolean matchesBlacklist(String urlString) {
        for (Pattern pattern : blacklistPatterns) {
            if (pattern.matcher(urlString).matches()) {
                return true;
            }
        }
        return false;
    }

    private static Pattern convertPatternToRegex(String pattern) {
        String regex = pattern.replace(".", "\\.")
                .replace("*", ".*");
        return Pattern.compile(regex);
    }

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        List<String> seeds = new ArrayList<>();
        List<String> stopWords = new ArrayList<>();
        List<String> blackListList = new ArrayList<>();
        Map<String, Integer> crawledNum = new HashMap<>();
        try {
            seeds = Files.readAllLines(Paths.get("links/" + args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            stopWords = Files.readAllLines(Paths.get("stopwords.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            blackListList = Files.readAllLines(Paths.get("links/blacklist.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        loadBlacklistPatterns("links/blacklist.txt");
        Set<String> blackList = new HashSet<>();
        for (String x : blackListList) {
            blackList.add(x);
        }

        String stopWordsPattern = "\\b(" + String.join("|", stopWords) + ")\\b";
        Pattern pattern = Pattern.compile(stopWordsPattern, Pattern.CASE_INSENSITIVE);

        String coordinatorArg = flameContext.getKVS().getCoordinator();

        FlameRDD urlQueue = flameContext.parallelize(seeds);
        while (urlQueue.count() > 0) {
            String lastTable = urlQueue.getTableName();
            urlQueue = urlQueue.flatMap(urlAndDepth -> {
                // System.out.println(urlAndDepth);
                String[] parts = urlAndDepth.split(" ");
                List<String> out = new ArrayList<>();
                int depth = 0;
                String urlString = "";
                if (parts.length == 1) {
                    urlString = urlAndDepth;
                }
                else if(parts.length == 2){
                    urlString = parts[0];
                    try {
                    depth = Integer.parseInt(parts[1]);
                    }catch(Exception e) {
                        return out;
                    }
                }
                else {
                    return out;
                }

                if(depth == maxDepth) {
                    return out;         //too deep 
                }



                KVSClient kvs = new KVSClient(coordinatorArg);

                String rowKey = Hasher.hash(urlString);

                if (!isAsciiString(urlString)) {
                    return out;
                }

                if (kvs.existsRow("pt-crawl", rowKey)) {
                    return out;
                }

                URI baseUri = new URI(urlString);
                parts = URLParser.parseURL(urlString);

                if (parts[1] == null) {
                    return out;
                }

                try {
                    if (matchesBlacklist(urlString)) {
                        return out;
                    }
                } catch (Exception e) {
                    System.out.println("moving_forward");
                }

                String hostKey = Hasher.hash(parts[1]);
                Row hostRow = kvs.getRow("hosts", hostKey);
                if (hostRow == null) {
                    hostRow = new Row(hostKey);
                }
                String robotsContent = hostRow.get("robots");
                if (robotsContent == null) {
                    try {
                        robotsContent = fetchRobots(baseUri);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return out;
                    }
                    // if(robotsContent == "") { // invalid url or sm
                    // return out;
                    //// }
                    hostRow.put("robots", robotsContent);
                    kvs.putRow("hosts", hostRow);
                }
                // System.out.println("here2");
                int checkRobotsVal = checkRobots(baseUri, urlString, hostRow, out, kvs);
                if (checkRobotsVal == 0 || checkRobotsVal == 3) {
                    System.out.println(checkRobotsVal);
                    return out;
                }

                if (checkRobotsVal == 2) {
                    out.add(urlString);
                    return out;
                }

                try {
                    URL baseUrl = baseUri.toURL();
                    Row row = sendHeadRequest(urlString, baseUri, baseUrl, rowKey, hostKey, kvs, out);
                    if (row == null) {
                        return out;
                    }
                    if (row != null) {
                        String len = row.get("length");
                        if (len == null) {
                            return out;
                        }
                        if (row != null) {

                            int code = 200;
                            // logger.error("Got code=" + code);
                            if (code == 200) {
                                HttpURLConnection connection = (HttpURLConnection) baseUrl.openConnection();
                                connection = (HttpURLConnection) baseUrl.openConnection(); // Create a new connection
                                                                                           // for GET request
                                connection.setRequestMethod("GET");
                                connection.setRequestProperty("User-Agent", USER_AGENT);
                                connection.setInstanceFollowRedirects(false);
                                connection.setRequestProperty("Accept-Language", "en");
                                connection.setConnectTimeout(5000); // Set connect timeout to 5 seconds
                                connection.setReadTimeout(5000); // Set read timeout to 5 seconds
                                kvs.put("hosts", hostKey, LAST_ACCESSED_COL, Long.toString(System.currentTimeMillis()));

                                connection.connect();
                                byte[] pageContent;
                                try (InputStream inputStream = connection.getInputStream()) {
                                    pageContent = inputStream.readAllBytes();
                                } catch (Exception e) {
                                    return out;
                                }

                                String s = new String(pageContent, StandardCharsets.UTF_8);

                                

                                // remove stop words and script. punctuation.
                                s = s.replaceAll("<(?:script)\\b[^<]*(?:(?!</(?:script)>)<[^<]*)*</(?:script)\\s*>",
                                        " ");
                                s = s.replaceAll("<[^>]*>", " ");
                                s = s.replaceAll("<style\\b[^<]*(?:(?!<\\/style>)<[^<]*)*<\\/style\\s*>", " ");
                                // s =
                                s.replaceAll("<noscript\\b[^<]*(?:(?!<\\/script>)<[^<]*)*<\\/noscript\\s*", " ");
                                // s = s.replaceAll("<meta\\b[^<]*(?:(?!<\\/style>)<[^<]*)*<\\/meta\\s*>", " ");
                                s = s.replaceAll("[^a-zA-Z0-9\\s]", " ");
                                String words = s;
                                s = s.replaceAll("content|div|menu|width|height|main|contents|sidebar|hide|navigation", " ");
                                s = pattern.matcher(s).replaceAll(" ");


                                s = s.replaceAll("\\s+", " ");
                                words = words.replaceAll("\\s+", " ").toLowerCase().trim();
                                
                                s = s.toLowerCase().trim();
   
                                row.put("d̶̅̈e̶̩̔s̴͚̾ċ̷̑r̵͆̈́i̵̝͐p̷͝tion", extractMetaDesc(pageContent, words));

                                // if (s.split(" ").length < 50)
                                //     return out; // too short the page

                                row.put("page", s.getBytes());

                                Set<String> distSet = new HashSet<>();
                                
                                for (String url : extractUrls(pageContent)) {
                                    URI uri = normalizeUrl(url, baseUri);
                                    if (uri == null) {
                                        continue;
                                    }
                                    distSet.add(uri.toString());
                                }
                                    
                                String urls = distSet.stream()
                                                .collect(Collectors.joining(","));

                                                
                                for (String url : distSet) {
                                    out.add(url + " " + Integer.toString((depth + 1)));
                                }

                                if(!urls.isEmpty()) {
                                    row.put("urls", urls);
                                }

                                row.put("t̷̺̀î̸͌t̴̘̉l̸͂͌ė̵͛", extractTitle(pageContent));
                                kvs.putRow("pt-crawl", row);
                                // pushQueue.add(row);
                                // if(pushQueue.size() > 100) {
                                //     kvs.batchPut("pt-crawl", pushQueue);
                                //     pushQueue.clear();
                                // }
                                // System.out.println("COMPLETED");
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Error processing URL: " + urlString);
                }
                return out;
            });
            KVSClient kvs = new KVSClient(coordinatorArg);
            kvs.delete(lastTable);
            // Thread.sleep(300);
            // kvs.delete()
        }

        flameContext.output("OK");
    }

    private static boolean isAsciiString(String str) {
        for (char c : str.toCharArray()) {
            if (c > 127) {
                return false;
            }
        }
        return true;
    }

    // Returns null if we shouldn't send GET request.
    private static Row sendHeadRequest(String urlString, URI baseUri, URL baseUrl, String rowKey,
            String hostKey, KVSClient kvs, List<String> out) throws IOException {
        kvs.put("hosts", hostKey, LAST_ACCESSED_COL, Long.toString(System.currentTimeMillis()));

        // Use this to indicate IOException.
        int code = -1;
        long contentLength = -1;
        String contentType = null;
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) baseUrl.openConnection();
            connection.setRequestMethod("HEAD");
            connection.setRequestProperty("Accept-Language", "en");
            connection.setRequestProperty("User-Agent", USER_AGENT);
            connection.setInstanceFollowRedirects(false);

            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);

            connection.connect();

            code = connection.getResponseCode();
            // System.out.println(Integer.toString(code));
            contentType = connection.getContentType();
            contentLength = connection.getContentLengthLong();
            if(contentLength <= 0) {
                return null;
            }
            String langs = connection.getHeaderField("Content-Language");

            if (langs != null && !langs.contains("en"))
                return null;

        } catch (Exception e) {
            System.out.println("Timeout occurred while connecting to " + baseUrl);
            return null;
        }

        finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        Row row = createRow(rowKey, contentLength, urlString, code, contentType);
        // logger.error("Got code=" + code + ", contentType=" + contentType + ",
        // contentLength="
        // + contentLength);
        kvs.putRow("pt-crawl", row);
        if (isRedirect(code)) {
            String redirectedUrl = connection.getHeaderField("Location");
            // logger.error("Redirected to '" + redirectedUrl + "'");
            if (redirectedUrl != null && !redirectedUrl.isBlank()) {
                URI redirectedUri = normalizeUrl(redirectedUrl, baseUri);
                if (redirectedUri != null) {
                    out.add(redirectedUri.toString());
                    // logger.error("Try redirected URL '" + redirectedUri + "' in the next round");
                }
            }
            return null;
        }
        if (code != HttpURLConnection.HTTP_OK || contentType == null
                || !contentType.contains("text/html")) {
            // logger.error("Skipped due to code or content type");
            return null;
        }
        return row;
    }

    // TODO: if value contains escaped quotes, e.g. \" or \', it might not be
    // working.
    private static final Pattern htmlOpenTagARegex = Pattern.compile(
            "<\\s*(?i:a)(?<attributes>(?:\\s+\\w+(?:\\s*=\\s*(?:\".*?\"|'.*?'|[^'\">\\s]+))?)*)\\s*>");
    private static final Pattern htmlAttributesRegex = Pattern.compile(
            "\\s+(?<key>\\w+)(?:\\s*=\\s*(?:\"(?<valueD>.*?)\"|'(?<valueS>.*?)'|(?<value>[^'\">\\s]+)))?");

    private static List<String> extractUrls(byte[] pageContent) {
        List<String> out = new ArrayList<>();
        String content = new String(pageContent, StandardCharsets.UTF_8);
        content.replaceAll("\\s", " ");
        Matcher aTagMatcher = htmlOpenTagARegex.matcher(content);
        while (aTagMatcher.find()) {
            String attributes = aTagMatcher.group("attributes");
            if (attributes == null || attributes.isBlank()) {
                continue;
            }
            Matcher attributeMatcher = htmlAttributesRegex.matcher(attributes);
            while (attributeMatcher.find()) {
                String key = attributeMatcher.group("key");
                if (key == null || !key.equalsIgnoreCase("href")) {
                    continue;
                }
                String url = attributeMatcher.group("valueD");
                if (url == null || url.isBlank()) {
                    url = attributeMatcher.group("valueS");
                    if (url == null || url.isBlank()) {
                        url = attributeMatcher.group("value");
                    }
                }
                if (url == null || url.isBlank()) {
                    continue;
                }
                out.add(url);
            }
        }
        return out;
    }

    private static URI normalizeUrl(String url, URI base) {
        try {
            URI uri = new URI(url);
            if (base != null) {
                uri = base.resolve(uri);
            } else {
                uri = uri.normalize();
            }
            String host = uri.getHost();
            if (host == null) {
                return null;
            }
            String path = uri.getPath();
            if (path == null || path.isEmpty()) {
                path = "/";
            }
            if (shouldIgnore(path)) {
                return null;
            }
            String scheme = uri.getScheme();
            if (scheme == null || scheme.equalsIgnoreCase("http")) {
                scheme = "http";
            } else if (scheme.equalsIgnoreCase("https")) {
                scheme = "https";
            } else {
                return null;
            }
            String userInfo = uri.getUserInfo();
            int port = uri.getPort();
            if (port == -1) {
                port = scheme.equals("http") ? 80 : 443;
            }
            String query = uri.getQuery();
            return new URI(scheme, userInfo, host, port, path, query, null);
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            return null;
        } catch (Exception e) {
            System.out.println("Exception in Crawler");
            e.printStackTrace();
            return null;
        }

    }

    private static boolean isRedirect(int code) {
        switch (code) {
            case HttpURLConnection.HTTP_MOVED_PERM:
            case HttpURLConnection.HTTP_MOVED_TEMP:
            case HttpURLConnection.HTTP_SEE_OTHER:
            case 307:
            case 308:
                return true;
        }
        return false;
    }

    private static Row createRow(String rowKey, long contentLength, String urlString, int code,
            String contentType) {
        Row row = new Row(rowKey);
        row.put("url", urlString);
        row.put("responseCode", Integer.toString(code));
        if (contentType != null) {
            row.put("contentType", contentType);
        }
        if (contentLength >= 0) {
            row.put("length", Long.toString(contentLength));
        }
        return row;
    }

    private static String fetchRobots(URI baseUri) throws URISyntaxException, IOException {
        URL baseUrl = new URI(baseUri.getScheme(), null, baseUri.getHost(), baseUri.getPort(),
                "/robots.txt", null, null).toURL();
        HttpURLConnection connection = (HttpURLConnection) baseUrl.openConnection();
        int code;
        try {
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", USER_AGENT);
            connection.setRequestProperty("Accept-Language", "en");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();

            code = connection.getResponseCode();
        }

        catch (Exception e) {
            return "";
        }

        // logger.error("Got code=" + code);
        if (code == HttpURLConnection.HTTP_OK) {
            try (InputStream inputStream = connection.getInputStream()) {
                String ret = new String(inputStream.readAllBytes());
                connection.disconnect();
                return ret;
            }
        }
        return "";
    }

    private static class RobotFileRule {
        final String path;
        final boolean allow;

        public RobotFileRule(String path, boolean allow) {
            this.path = path;
            this.allow = allow;
        }

    }

    private static class RobotFileBlock {
        final String userAgent;
        final List<RobotFileRule> rules = new ArrayList<>();
        Double crawlDelay;

        public RobotFileBlock(String userAgent) {
            this.userAgent = userAgent;
        }
    }

    private static Map<String, RobotFileBlock> parseRobotFile(String content) {
        Map<String, RobotFileBlock> out = new HashMap<>();
        // Try to be permissive: if not starts with user-agent directive, treat it as *.
        RobotFileBlock current = out.computeIfAbsent("*", RobotFileBlock::new);
        if (content == null) {
            content = "";
        }
        for (String line : content.split("\\r|\\n")) {
            // logger.error("robots.txt got line '" + line + "'");
            String[] parts = line.trim().split(":", 2);
            if (parts.length != 2 || parts[1].isBlank()) {
                continue;
            }
            String key = parts[0].trim(), value = parts[1].trim();
            // logger.error("robots.txt got key='" + key + "', value='" + value + "'");
            if (key.equalsIgnoreCase("user-agent")) {
                current = out.computeIfAbsent(value.toLowerCase(), RobotFileBlock::new);
            } else if (key.equalsIgnoreCase("allow") || key.equalsIgnoreCase("disallow")) {
                current.rules.add(new RobotFileRule(value, key.equalsIgnoreCase("allow")));
            } else if (key.equalsIgnoreCase("crawl-delay")) {
                // If multiple crawl-delay are specified, the first one takes precedence.
                if (current.crawlDelay == null) {
                    try {
                        current.crawlDelay = Double.valueOf(value);
                    } catch (Exception e) {
                        // Try to be permissive.
                        e.printStackTrace();
                    }
                }
            }
        }
        return out;
    }

    // Returns true if we should proceed.

    // error 0 not allowed
    // error 2 wait for crawl delay
    // error 3 too many pages for this host
    // error 4 something wrong with the put
    // error 1 all good
    private static int checkRobots(URI baseUri, String baseUrl, Row hostRow, List<String> out, KVSClient kvs) {
        String robotsContent = hostRow.get("robots");
        double crawlDelay = 0;
        // System.out.println("here68");
        if (!robotsContent.equals("")) {
            Map<String, RobotFileBlock> parsedRobotsFile = parseRobotFile(robotsContent);

            RobotFileBlock block = findRobotFileBlock(parsedRobotsFile, USER_AGENT);

            if (block != null) {
                if (block.crawlDelay != null) {
                    crawlDelay = block.crawlDelay;
                }
                for (RobotFileRule rule : block.rules) {
                    String path = baseUri.getPath();
                    if (path.startsWith(rule.path)) {
                        if (!rule.allow) {
                            // logger.error("URL '" + baseUri + "' is blocked by rule " + rule.path);
                            return 0;
                        }
                        break;
                    }
                }
            }
        }
        // System.out.println("here7");
        String lastAccessedTimeMsString = hostRow.get(LAST_ACCESSED_COL);
        if (lastAccessedTimeMsString != null) {
            long lastAccessedTimeMs = Long.parseLong(lastAccessedTimeMsString);
            long currentTimeMs = System.currentTimeMillis();
            // System.out.println(Double.toString(currentTimeMs - lastAccessedTimeMs));
            if (currentTimeMs < lastAccessedTimeMs + 1000 * crawlDelay) {
                out.add(baseUrl);
                // logger.error("URL '" + baseUrl + "' is throttled, try again in next round: "
                // + currentTimeMs + " vs " + lastAccessedTimeMs + " + 1000 * " + crawlDelay);
                return 2;
            }
        }
        // System.out.println("here8");
        // if(crawledNum.containsKey(host)) {
        // int temp = crawledNum.get(host);
        // if(temp > 1000000) {
        // return 3;
        // }
        // crawledNum.put(host, temp + 1);
        // }
        // else {
        // crawledNum.put(host, 1);
        // }

        // SAVE FOR BATCH PUT
        // try {
        // numSites = Integer.parseInt(snumSites);
        // }catch (Exception e) {
        // System.out.println("NUM SITES NOT PARSED");
        // return 3;
        // }

        // else
        // try {
        // kvs.put("hosts", Hasher.hash(baseUrl), "numCrawled",
        // Integer.toString(numSites + 1));
        // } catch(Exception e) {
        // return 4;
        // }

        return 1;
    }

    private static RobotFileBlock findRobotFileBlock(Map<String, RobotFileBlock> parsedRobotsFile,
            String userAgent) {
        Map.Entry<String, RobotFileBlock> bestMatch = null;
        for (Map.Entry<String, RobotFileBlock> entry : parsedRobotsFile.entrySet()) {
            if (robotUserAgentMatches(entry.getKey(), userAgent)) {
                if (bestMatch == null || bestMatch.getKey().length() < entry.getKey().length()) {
                    bestMatch = entry;
                }
            }
        }
        return bestMatch == null ? null : bestMatch.getValue();
    }

    private static boolean robotUserAgentMatches(String robotUserAgent, String userAgent) {
        return robotUserAgent.equals("*") || userAgent.toLowerCase().contains(robotUserAgent);
    }

    private static boolean shouldIgnore(String path) {
        path = path.toLowerCase();
        if (path.endsWith(".jpg") || path.endsWith(".jpeg") || path.endsWith(".gif")
                || path.endsWith(".png") || path.endsWith(".txt")) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws URISyntaxException, IOException {
    }
}