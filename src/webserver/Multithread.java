package webserver;

import java.io.BufferedReader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Multithread extends Thread {

	Socket clientSocket;
	String directory;
	Server main;
	String domain;
	int portNum;

	HashMap<String, Route> routes;
	Map<String, SessionImpl> Sessions;

	public Multithread(Socket s, String d, HashMap<String, Route> x, Server S, int p, Map<String, SessionImpl> sesh) {
		clientSocket = s;
		directory = d;
		routes = x;
		main = S;
		portNum = p;
		Sessions = sesh;
	}

	public String generateSessionId(int length) {
		String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
		SecureRandom random = new SecureRandom();
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(characters.charAt(random.nextInt(characters.length())));
		}
		return sb.toString();
	}

	public static Map<String, String> matchPath(String url, String pathPattern) {
		String[] urlParts = url.split("/");
		String[] patternParts = pathPattern.split("/");
		Map<String, String> params = new HashMap<>();

		if (urlParts.length != patternParts.length) {
			return null; // The URL and the pattern do not match in length.
		}

		for (int i = 0; i < patternParts.length; i++) {
			if (patternParts[i].startsWith(":")) {

				String paramName = patternParts[i].substring(1);
				params.put(paramName, urlParts[i]);
			} else if (!urlParts[i].equals(patternParts[i])) {

				return null;
			}
		}

		return params;
	}

	public static Map<String, String> parseQueryParameters(String query) {
		Map<String, String> queryParameters = new HashMap<>();
		try {

			String[] pairs = query.split("&");
			for (String pair : pairs) {

				int idx = pair.indexOf("=");

				String name = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
				String value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");

				queryParameters.put(name, value);
			}
		} catch (Exception e) {
			// Handle exceptions related to decoding or parsing
			e.printStackTrace();
		}
		return queryParameters;
	}

	@Override
	public void run() {
		try {

			InputStream inputStream = clientSocket.getInputStream();
			OutputStream outputStream = clientSocket.getOutputStream();

			byte[] data = new byte[8192];
			int singleByte;
			int pointer = 0;
			boolean endOfHeader = false;

			PrintWriter out = new PrintWriter(outputStream, true);
			// Ensure all data is sent

			while ((singleByte = inputStream.read()) != -1) {
				data[pointer] = (byte) singleByte;
				if (pointer >= 3) {
					if (data[pointer] == 10 && data[pointer - 1] == 13 && data[pointer - 2] == 10
							&& data[pointer - 3] == 13) {
						endOfHeader = true;
					}
				}

				if (endOfHeader) {
					byte[] realData = Arrays.copyOfRange(data, 0, pointer + 1);

					BufferedReader reader1 = new BufferedReader(
							new InputStreamReader(new ByteArrayInputStream(realData)));

					String line = "";

					int count = 0;
					boolean keep = false;

					boolean hasError = false;
					String error = "";
					String[] toDo = null;
					boolean hasHost = false;
					byte[] tempData = new byte[0];
					Map<String, String> headers = new HashMap<String, String>();

					while ((line = reader1.readLine()) != null && !line.isEmpty()) {
						String[] values;
						if (count == 0) {
							values = line.split(" ");
						} else {
							values = line.split(":");
						}
						if (count == 0) {
							toDo = values;

							if (toDo.length < 3) {
								hasError = true;
								error = "400 Bad Request";
							} else if (!toDo[2].equals("HTTP/1.1")) {
								hasError = true;
								error = "505 HTTP Version Not Supported";
							}

							else if (!(toDo[0].equals("PUT") || toDo[0].equals("POST") || toDo[0].equals("GET")
									|| toDo[0].equals("HEAD"))) {
								hasError = true;
								error = "501 Not Implemented";
							}
						}

						count++;

						if (count != 0 && !(values.length < 2) && values[0].equals("Host")) {
							hasHost = true;
							domain = values[1].trim();
						}

						if (count != 0 && values[0].equals("Content-Length") && values.length > 1) {
							int length = Integer.parseInt(values[1].trim());
							if (length > 0) {
								tempData = new byte[length];
								for (int i = 0; i < length; i++)
									tempData[i] = (byte) inputStream.read();

							}

						}

						if (count != 0 && values.length == 2) {
							headers.put(values[0].toLowerCase(), values[1].trim());
						}

					}

					if (!hasError && !hasHost) {
						hasError = true;
						error = "400 Bad Request";
					}

					if (hasError) {
						out.print("HTTP/1.1 " + error + "\r\n");
						out.print("Content-Type: text/plain\r\n");
						out.print("Server: Server\r\n");
						out.print("Content-Length: 0\r\n");
						out.print("\r\n");
						out.flush();
					} else {

						String cookieHeader = headers.get("cookie");
						String sessionId = null;
						SessionImpl session = null;
						boolean newSesh = false;
						if (cookieHeader != null) {
							String[] cookies = cookieHeader.split(" ");
							for (String cookie : cookies) {
								if (cookie.trim().startsWith("SessionID=")) {
									sessionId = cookie.trim().substring("SessionID=".length());
									break;
								}
							}
						}

						if (sessionId != null && Sessions.containsKey(sessionId)) {
							if (!Sessions.get(sessionId).isExpired()) {
								session = Sessions.get(sessionId);
								// Update last accessed time
								session.setLastAccessedTime(System.currentTimeMillis());
							} else {
								session = null;
							}
						}

						if (session == null) {
							// Generate a new session ID with 120 bits of randomness
							newSesh = true;
							sessionId = generateSessionId(20); // Implement this method to generate a random ID
							session = new SessionImpl(sessionId);
							Sessions.put(sessionId, session);

						}

						String[] content = toDo;
						boolean hasRoute = false;

						for (String X : routes.keySet()) {

							if (X.startsWith(content[0])) {
								String[] r = X.split(" ");

								String lmao = content[1];
								String[] params = lmao.split("\\?+");

								String param;
								String queries = "";

								param = params[0];
								if (params.length == 2) {
									queries = params[1];
								}

								if (r[1].equals(content[1]) || (matchPath(param, r[1])) != null) {
									//System.out.println(queries);
									hasRoute = true;
									Map<String, String> parameters = matchPath(param, r[1]);
									Map<String, String> querieParams = null;

									if (headers.containsKey("content-type") && headers.get("content-type")
											.equals("application/x-www-form-urlencoded")) {

										if (queries.equals(""))
											queries = new String(tempData, StandardCharsets.UTF_8);

										else
											queries += ("&" + new String(tempData, StandardCharsets.UTF_8));


									}

									if (!queries.equals(""))
										querieParams = parseQueryParameters(queries);

									InetSocketAddress remoteAddress = new InetSocketAddress(
											clientSocket.getInetAddress(), clientSocket.getPort());
									String method = content[1];
									String what = content[2];
									ResponseImpl response = new ResponseImpl(out, outputStream, method, what);

									RequestImpl request = new RequestImpl(content[0], content[1], content[2], headers,
											querieParams, parameters, remoteAddress, tempData, main, session, response,
											newSesh);

									try {

										// if (content[0].equals("GET")) {

										Route router = routes.get(X);
										String send = null;
										Object obj = router.handle(request, response);
										if (obj != null) {
											send = obj.toString();
											// System.out.println(send);
										}

										if (response.getWrite() != 0) {
											clientSocket.close();
										}

										if (send == null && response.getWrite() == 0 && response.getBody() != null) {
											send = new String(response.getBody(), StandardCharsets.UTF_8);

										}

										if (send != null && response.getWrite() == 0) {

											byte[] responses = send.getBytes(StandardCharsets.UTF_8);

											String type = "text/html";
											if (method.equals("GET")) {
												type = "text/plain";
											}

											response.bodyAsBytes(responses);

											out.print("HTTP/1.1 200 OK\r\n");
											out.print("Content-Type: " + type + "\r\n");

											out.print("Server: Server\r\n");
											out.print("Content-Length: " + String.valueOf(responses.length) + "\r\n");
											for (String S : response.getHeaders())
												out.print(S + "\r\n");

											out.print("\r\n");
											out.flush();
											outputStream.write(responses);
											outputStream.flush();
											response.body(send);

										}
										break;
									} catch (Exception e) {

										if (response.getWrite() == 0) {
											String errorResponse = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
											out.write(errorResponse);
											out.flush();
											break;
										} else {
											clientSocket.close();
											break;
										}
									}

								}
							}
						}

						if (!hasRoute && (!keep && content[0].equals("GET")) || (!keep && content[0].equals("HEAD"))) {
							String file = content[1];

							if (content[1].contains("..")) {
								out.print("HTTP/1.1 403 Forbidden\r\n");
								out.print("Content-Type: text/plain\r\n");
								out.print("Server: Server\r\n");
								out.print("Content-Length: 0\r\n");
								out.print("\r\n");
								out.flush();
								keep = true;

							}

							else {
								String location = directory + file;
								String contentType = "";

								if (location.endsWith("html"))
									contentType = "text/html";

								else if (location.endsWith("jpg") || location.endsWith("jpeg"))
									contentType = "image/jpeg";

								else if (location.endsWith("txt"))
									contentType = "text/plain";

								else
									contentType = "application/octet-stream";

								Path path = Paths.get(location);

								if (!Files.exists(path)) {

									out.print("HTTP/1.1 404 Not Found\r\n");
									out.print("Content-Type: text/plain\r\n");
									out.print("Server: Server\r\n");
									out.print("Content-Length: 0\r\n");
									out.print("\r\n");
									out.flush();
									keep = true;
								} else if (!Files.isReadable(path)) {
									out.print("HTTP/1.1 403 Forbidden\r\n");
									out.print("Content-Type: text/plain\r\n");
									out.print("Server: Server\r\n");
									out.print("Content-Length: 0\r\n");
									out.print("\r\n");
									out.flush();
									keep = true;

								} else {
									byte[] material = Files.readAllBytes(path);
									int contentLength = material.length;

									out.print("HTTP/1.1 200 OK\r\n");
									out.print("Content-Type: " + contentType + "\r\n");
									out.print("Server: Server\r\n");
									out.print("Content-Length: " + contentLength + "\r\n");
									out.print("\r\n"); // End of headers

									out.flush();

									if (!content[0].equals("HEAD")) {
										outputStream.write(material);
										outputStream.flush();
									}

									keep = true;

								}

							}

						}

					}

					data = new byte[8192];
					pointer = -1;
					endOfHeader = false;

				}

				pointer++;

			}

			clientSocket.close();

		} catch (

		Exception e) {

		}
	}

}
