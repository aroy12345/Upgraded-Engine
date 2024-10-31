package webserver;

import java.io.FileInputStream;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

public class Server {

	public static Server field = null;
	public static boolean myFlag = false;
	public int portNum = 80;
	public int securePortNum = 80;
	public String loc;
	public static boolean isSecure = false;
	Map<String, SessionImpl> sessions = new ConcurrentHashMap<>();

	public void startSessionExpirationTask() {
		new Thread(() -> {
			while (true) {
				try {
					// Iterate over the session map and remove expired sessions

					sessions.entrySet().forEach(entry -> {
						SessionImpl session = entry.getValue();
						if (session.isExpired()) {
							// System.out.println("Expiring session: ID=" + entry.getKey() + ", Last Accessed Time="
							// 		+ session.lastAccessedTime() + ", Max Active Interval="
							// 		+ session.getMaxActiveInterval() + " seconds");
						}
					});
					sessions.entrySet().removeIf(entry -> entry.getValue().isExpired());

					// Sleep for a defined interval before checking again
					Thread.sleep(1000); // Check every 5 seconds
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		}).start();
	}

	public Server() {
		startSessionExpirationTask();
	
	}

	public static class staticFiles {
		public static void location(String S) {
			if (field == null) {
				field = new Server();
			}
			field.loc = S;
		}

	}

	private static BlockingQueue<Socket> socketQueue = new ArrayBlockingQueue<>(100000000);
	HashMap<String, Route> mappings = new HashMap<String, Route>();

	public static void get(String S, Route R) {

		if (field == null) {
			field = new Server();
		}

		field.mappings.put("GET " + S, R);
		if (!myFlag) {
			myFlag = true;
			new Thread(() -> {
				try {

					field.run(); // Start server in its own thread.
				} catch (IOException e) {

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}).start();
		}
	}

	public static void post(String S, Route R) {
		if (field == null) {
			field = new Server();
		}
		field.mappings.put("POST " + S, R);
		if (!myFlag) {

			myFlag = true;
			new Thread(() -> {
				try {

					field.run(); // Start server in its own thread.
				} catch (IOException e) {

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}).start();
		}
	}

	public static void put(String S, Route R) {
		if (field == null) {
			field = new Server();
		}
		field.mappings.put("PUT " + S, R);
		if (!myFlag) {
			myFlag = true;
			new Thread(() -> {
				try {

					field.run(); // Start server in its own thread.
				} catch (IOException e) {

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}).start();
		}
	}

	public static void port(int num) {

		if (field == null) {
			field = new Server();

		}

		field.portNum = num;
		isSecure = false;

	}

	public static void securePort(int securePortNo) throws Exception {

		if (field == null) {
			field = new Server();

		}

		field.securePortNum = securePortNo;
		isSecure = true;
	}

	public void run() throws Exception {

		ServerSocket ssock;

		new Thread(() -> {
			try {
				ServerSocket httpSocket = new ServerSocket(portNum); // HTTP
				//System.out.println("HTTP server running on port " + portNum);
				serverLoop(httpSocket);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();

		// Start HTTPS server thread
		new Thread(() -> {
			try {
				if (isSecure) {
					String pwd = "secret";
					KeyStore keyStore = KeyStore.getInstance("JKS");
					keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
					KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
					keyManagerFactory.init(keyStore, pwd.toCharArray());
					SSLContext sslContext = SSLContext.getInstance("TLS");
					sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
					ServerSocketFactory factory = sslContext.getServerSocketFactory();
					ServerSocket sslSocket = factory.createServerSocket(securePortNum); // HTTPS
					//System.out.println("HTTPS server running on port " + securePortNum);
					serverLoop(sslSocket);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();

	}

	public void serverLoop(ServerSocket serverSocket) throws Exception {
		while (true) {
			Socket clientSocket = serverSocket.accept();
			socketQueue.add(clientSocket);
			try {
				Socket T = socketQueue.take();
				Multithread run = new Multithread(T, loc, mappings, this, portNum, sessions);
				run.start();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}