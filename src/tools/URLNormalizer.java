package tools;

import java.net.MalformedURLException;


import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLNormalizer {
	private static final Pattern URL_PATTERN = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"",
			Pattern.CASE_INSENSITIVE);
	private static final Set<String> IGNORED_EXTENSIONS = new HashSet<>(
			Arrays.asList(".jpg", ".jpeg", ".gif", ".png", ".txt"));

	public static boolean isValidUrl(String url) {
		try {
			URL parsedUrl = new URL(url);
			String protocol = parsedUrl.getProtocol();
			if (!protocol.equals("http") && !protocol.equals("https")) {
				return false;
			}

			String path = parsedUrl.getPath().toLowerCase();
			for (String ext : IGNORED_EXTENSIONS) {
				if (path.endsWith(ext)) {
					return false;
				}
			}

			return true;
		} catch (MalformedURLException e) {
			System.err.println("Invalid URL format: " + url);
			return false;
		}
	}

	public static Set<String> extractUrls(String content, String baseUrl) {
		Set<String> urls = new HashSet<>();
		Matcher matcher = URL_PATTERN.matcher(content);
		while (matcher.find()) {
			String url = matcher.group(1);
			try {

				URL resolvedUrl = new URL(new URL(baseUrl), url);
				urls.add(resolvedUrl.toString());
			} catch (Exception e) {
				System.err
						.println("Error resolving URL '" + url + "' against base '" + baseUrl + "': " + e.getMessage());
			}
		}
		return urls;
	}

	public static String extractHost(String url) {
		try {
			URL parsedUrl = new URL(url);
			return parsedUrl.getHost();
		} catch (Exception e) {
			System.err.println("Error extracting host from URL '" + url + "': " + e.getMessage());
			return null;
		}
	}

	public static String normalizeUrl(String url, String baseUrl) throws MalformedURLException {
		if (url == null || url.isEmpty()) {
			return null;
		}

		int fragmentIndex = url.indexOf('#');
		if (fragmentIndex != -1) {
			url = url.substring(0, fragmentIndex);
		}

		URL base = new URL(baseUrl);
		URL resolvedUrl = new URL(base, url);

		int port = resolvedUrl.getPort() != -1 ? resolvedUrl.getPort() : resolvedUrl.getDefaultPort();
		String portString = ":" + port;

		String path = resolvedUrl.getFile();
		if (path.equals("") || path.equals("/")) {
			path = "/";
		}

		return resolvedUrl.getProtocol() + "://" + resolvedUrl.getHost() + portString + path;
	}

	public static Set<String> normalizeAndFilterUrls(Set<String> urls, String baseUrl) throws URISyntaxException {
		Set<String> normalizedUrls = new HashSet<>();
		for (String url : urls) {
			try {
				String normalizedUrl = normalizeUrl(url, baseUrl);
				if (normalizedUrl != null) {

					normalizedUrls.add(normalizedUrl);
				}
			} catch (MalformedURLException e) {
				System.err.println("Error normalizing URL '" + url + "': " + e.getMessage());
			}
		}
		return normalizedUrls;
	}

	public static void main(String[] args) throws URISyntaxException {
		String baseUrl = "https://foo.com:8000/bar/xyz.html";
		Set<String> urls = new HashSet<>();
		urls.add("#abc");
		urls.add("blah.html#test");
		urls.add("../blubb/123.html");
		urls.add("/one/two.html");
		urls.add("http://elsewhere.com/some.html");

		Set<String> normalizedUrls = normalizeAndFilterUrls(urls, baseUrl);
		for (String url : normalizedUrls) {
			System.out.println(url);
		}
	}
}
