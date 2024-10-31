package tools;

import java.io.InputStream;


import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import flame.FlameContext;

public class RobotsExclusionProtocolHandler {

	private static final String ROBOTS_TXT_PATH = "/robots.txt";
	private final String host;
	private final FlameContext context;
	private Map<String, String> rules;
	private double crawlDelay;

	public RobotsExclusionProtocolHandler(FlameContext context, String host) {
		this.context = context;
		this.host = host;
		this.rules = new HashMap<>();
		this.crawlDelay = 1.0; // Default crawl delay
		parseRobotsTxt();
	}

	public double getCrawlDelay() {
		return (long) (1000 * crawlDelay);
	}

	private void parseRobotsTxt() {
		try {
			URL url = new URL("http://" + host + ROBOTS_TXT_PATH);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
				InputStream in = connection.getInputStream();
				byte[] data = new byte[8192];
				int pointer = 0;
				int singleByte;
				while ((singleByte = in.read()) != -1) {
					if (pointer == data.length) {
						data = Arrays.copyOf(data, data.length * 2);
					}
					data[pointer++] = (byte) singleByte;
				}
				in.close();

				String pageContent = new String(data, 0, pointer);
				parseContent(pageContent);
			}
		} catch (Exception e) {
			System.err.println("Error fetching or parsing robots.txt for host " + host + ": " + e.getMessage());
		}
	}

	private void parseContent(String content) {
		String[] lines = content.split("\n");
		boolean parseRules = false;
		for (String line : lines) {
			// System.out.println(line);
			if (line.startsWith("User-agent:")) {
				String userAgent = line.substring("User-agent:".length()).trim();
				parseRules = "cis5550-crawler".equals(userAgent) || "*".equals(userAgent);
			} else if (parseRules) {
				if (line.startsWith("Allow:")) {
					rules.put(line.substring("Allow:".length()).trim(), "Allow");
				} else if (line.startsWith("Disallow:")) {
					rules.put(line.substring("Disallow:".length()).trim(), "Disallow");
				} else if (line.startsWith("Crawl-delay:")) {
					try {
						crawlDelay = Double.parseDouble(line.substring("Crawl-delay:".length()).trim());
					} catch (NumberFormatException e) {
						System.err.println("Invalid Crawl-delay value");
					}
				}
			}
		}

		for (Map.Entry<String, String> entry : rules.entrySet()) {
			// System.out.println(entry.getKey() + " = " + entry.getValue());
		}

	}

	public boolean isAllowed(String url) throws MalformedURLException {

		boolean allowed = false;
		boolean notAllowed = false;
		int length = -1;
		URL urlObject = new URL(url);
		String path = urlObject.getPath();

		// System.out.println(path + " path");
		for (Map.Entry<String, String> entry : rules.entrySet()) {
			if (path.startsWith(entry.getKey())) {
				if ("Allow".equals(entry.getValue()) && entry.getKey().length() >= length) {
					allowed = true;
					notAllowed = false;
					length = entry.getKey().length();
				}

				if ("Disallow".equals(entry.getValue()) && entry.getKey().length() > length) {
					notAllowed = true;
					allowed = false;
					length = entry.getKey().length();
				}
			}
		}

		if (allowed) {
			// System.out.println(url + " lol");
			return true;
		} else if (notAllowed) {
			// System.out.println(url + " Lmao");
			return false;
		} else {
			System.out.println(url + " lol");
			return true;
		}
	}

	public void respectCrawlDelay() throws InterruptedException {
		if (crawlDelay > 0) {
			long delayMillis = (long) (crawlDelay * 1000);
			Thread.sleep(delayMillis);
		}
	}
}
