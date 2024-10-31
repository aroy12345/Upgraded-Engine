package frontend;

import flame.FlameContext;
import flame.FlameContextImpl;
import objects.Website;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.time.Duration;
import java.time.Instant;

import static backend.routes.search;
import static webserver.Server.get;
import static webserver.Server.port;

public class routes {

    static int numResults = 10;

    public static void main(String args[]) {


        port(80);
        File directory = new File("pt-combinedscores");
        File[] files = directory.listFiles();
        assert files != null;
        List<String> fileNames = Arrays.stream(files).map(file -> "\"" + file.getName() + "\"").toList();
        List<String> fileNames_without_quotes = Arrays.stream(files).map(File::getName).toList();


        System.out.println(jaroWinklerDistance("chlorla", "chloral"));
        System.out.println(jaroWinklerDistance("chlorla", "induces"));
        String words = "[" + String.join(", ", fileNames) + "];";


        get("/", (req, res) -> {
            res.type("text/html");
            return """
                    <!DOCTYPE html>
                    <html lang="en">
                    <head>
                        <meta charset="UTF-8">
                        <meta name="viewport" content="width=device-width, initial-scale=1.0">
                        <title>Capybara Search</title>
                        <link rel="stylesheet" href="https://code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">
                        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
                        <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.min.js"></script>
                        <style>
                            body {
                                font-family: Arial, sans-serif;
                                display: flex;
                                justify-content: center;
                                align-items: center;
                                height: 100vh;
                                margin: 0;
                                background-color: #dbe4e0;
                            }
                            .search-container {
                                text-align: center;
                                background-color: #ffffff;
                                padding: 20px;
                                border-radius: 10px;
                                box-shadow: 0px 0px 10px 0px rgba(0,0,0,0.3);
                            }
                            h2 {
                                color: #3d5940;
                            }
                            input[type=text] {
                                width: 50%;
                                padding: 12px 20px;
                                margin: 8px 0;
                                box-sizing: border-box;
                                border: 2px solid #3d5940;
                                border-radius: 4px;
                                background-color: #f0f3f2;
                            }
                            .search-button {
                                background-color: #3d5940;
                                color: white;
                                padding: 14px 20px;
                                margin-top: 10px;
                                border: none;
                                border-radius: 20px;
                                cursor: pointer;
                                transition: background-color 0.3s;
                            }
                            .search-button:hover {
                                background-color: #5e8567;
                            }
                        </style>
                    </head>
                    <body>
                        <div class="search-container">
                            <h2>Capybara Search</h2>
                            <form action="/search" method="get">
                                <input type="text" placeholder="Query" name="query" id="searchInput">
                                <input type="hidden" name="page" value="1">
                                <button class="search-button" type="submit">Search</button>
                            </form>
                        </div>
                        <script>
                            $(function() {
                                var availableTags = """ + words + """
                                $("#searchInput").autocomplete({
                                                       source: function(request, response) {
                                                           var matches = $.map(availableTags, function(tag) {
                                                               if (tag.toLowerCase().startsWith(request.term.toLowerCase())) {
                                                                   return tag;
                                                               }
                                                           });
                                                           response(matches);
                                                       }
                                                   });
                            });
                        </script>
                    </body>
                    </html>
                    """;
        });

        get("/search", (req, res) -> {
            String query = req.queryParams("query");
            int page = req.queryParams().contains("page") ? Integer.parseInt(req.queryParams("page")) : 1;
            Instant start = Instant.now();
            List<Website> results = search(query, page);
            res.type("text/html");
            StringBuilder html = new StringBuilder("<!DOCTYPE html><html lang='en'><head>")
                    .append("<meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'>")
                    .append("<title>Search Results</title>")
                    .append("<style>")
                    .append("body { font-family: Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #dbe4e0; }")
                    .append(".search-container { text-align: center; background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0px 0px 10px 0px rgba(0,0,0,0.3); }")
                    .append("h2 { color: #3d5940; }")
                    .append("input[type=text], .page-button { width: 50%; padding: 12px 20px; margin: 8px 0; box-sizing: border-box; border: 2px solid #3d5940; border-radius: 4px; background-color: #f0f3f2; }")
                    .append(".search-button, .page-button { background-color: #3d5940; color: white; padding: 14px 20px; margin-top: 10px; border: none; border-radius: 20px; cursor: pointer; transition: background-color 0.3s; }")
                    .append(".search-button:hover, .page-button:hover { background-color: #5e8567; }")
                    .append("</style></head><body>")
                    .append("<div class='search-container'>")
                    .append("<h2>Results for '").append(query).append("'</h2><ul>");

            if (results.isEmpty()) {
                StringBuilder suggestion = new StringBuilder();
                for (String word : query.split(" ")) {
                    suggestion.append(lev(word, fileNames_without_quotes)).append(" ");
                }
                suggestion.delete(suggestion.length() - 1, suggestion.length());
                results = search(suggestion.toString(), page);
                html.append("<p>No results found for '").append(query).append("'. We've shown the results for '").append(suggestion).append("' instead.</p>");
            }

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            html.append("<p>Search completed in ").append(timeElapsed).append(" ms.</p>");


            int count = 0;
            for (Website website : results) {
                html.append("<li><a href='")
                        .append(website.getUrl()).append("'>")
                        .append(website.getTitle()).append("</a><p>")
                        .append(website.getDescription()).append("</p></li>");
                if (++count == numResults) {
                    break;
                }
            }


            if (page > 1) {
                html.append("<form action='/search' method='get' style='display: inline; margin-right: 10px;'>")
                        .append("<input type='hidden' name='query' value='").append(query).append("'>")
                        .append("<input type='hidden' name='page' value='").append(page - 1).append("'>")
                        .append("<button type='submit' class='page-button'>Previous</button>")
                        .append("</form>");
            }

            if (results.size() > numResults) {
                html.append("<form action='/search' method='get' style='display: inline;'>")
                        .append("<input type='hidden' name='query' value='").append(query).append("'>")
                        .append("<input type='hidden' name='page' value='").append(page + 1).append("'>")
                        .append("<button type='submit' class='page-button'>Next</button>")
                        .append("</form>");
            }
            html.append("</ul></div></body></html>");

            return html.toString();

        });
    }

    public static double jaroWinklerDistance(String s1, String s2) {
        int s1Len = s1.length();
        int s2Len = s2.length();

        if (s1Len == 0 || s2Len == 0) {
            return 0;
        }

        int matchDistance = Integer.max(s1Len, s2Len) / 2 - 1;

        boolean[] s1Matches = new boolean[s1Len];
        boolean[] s2Matches = new boolean[s2Len];

        int matches = 0;
        int transpositions = 0;

        for (int i = 0; i < s1Len; i++) {
            int start = Math.max(0, i - matchDistance);
            int end = Math.min(i + matchDistance + 1, s2Len);

            for (int j = start; j < end; j++) {
                if (s2Matches[j]) continue;
                if (s1.charAt(i) != s2.charAt(j)) continue;
                s1Matches[i] = true;
                s2Matches[j] = true;
                matches++;
                break;
            }
        }

        if (matches == 0) return 0;

        int k = 0;
        for (int i = 0; i < s1Len; i++) {
            if (!s1Matches[i]) continue;
            while (!s2Matches[k]) k++;
            if (s1.charAt(i) != s2.charAt(k)) transpositions++;
            k++;
        }

        double jaro = ((double) matches / s1Len + (double) matches / s2Len +
                (double) (matches - transpositions / 2.0) / matches) / 3.0;

        // Jaro-Winkler adjustment
        int prefixLength = 0;
        for (int i = 0; i < Math.min(s1Len, s2Len); i++) {
            if (s1.charAt(i) != s2.charAt(i)) break;
            prefixLength++;
        }

        return jaro + Math.min(0.1, 1.0 / s1Len) * prefixLength * (1 - jaro);
    }

    public static String lev(String target, List<String> strings) {
        String closest = null;
        double val = Integer.MIN_VALUE;

        for (String str : strings) {
            double sim = jaroWinklerDistance(target, str);
            if (sim > val) {
                val = sim;
                closest = str;
            }
        }
        return closest;
    }
}
