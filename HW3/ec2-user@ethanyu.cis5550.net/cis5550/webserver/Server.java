package cis5550.webserver;

import java.net.*;
import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.time.Instant;
import javax.net.ssl.*;
import javax.net.ServerSocketFactory;
import java.security.*;

import cis5550.tools.Logger;

public class Server {
    String dir;
    int port = 80, sPort = -1;
    static boolean flag;
    static final int NUM_WORKER_THREADS = 100;
    private static final Logger logger = Logger.getLogger(Server.class);
    private ServerSocket serverSocket, serverSocketTLS;
    private LinkedBlockingQueue<Socket> queue = new LinkedBlockingQueue<>();
    ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    static Server instance;
    static HashMap<String, List<ServerRoute>> routingTable = new HashMap<>();

    class SessionReaper implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    for (String key : instance.sessions.keySet()) {
                        SessionImpl session = (SessionImpl) instance.sessions.get(key);
                        if (!session.valid || System.currentTimeMillis() - session.lastAccessedTime > session.maxActiveInterval * 1000L) {
                            session.invalidate();
                            instance.sessions.remove(key);
                        }
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    class Responder implements Runnable {
        @Override
        public void run() {
            while (true) {
                Socket socket;
                try {
                    socket = queue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                logger.info("New connection from " + socket.getRemoteSocketAddress());
                try {
                    while (!socket.isClosed() || socket.getInputStream().available() > 0) {
                        byte[] buffer = new byte[1024];
                        int pointer = 0;
                        while (true) {
                            byte b = (byte) socket.getInputStream().read();
                            if (b == -1) {
                                break;
                            }
                            buffer[pointer++] = b;
                            if (pointer > 3 && buffer[pointer - 1] == 10 && buffer[pointer - 2] == 13 && buffer[pointer - 3] == 10 && buffer[pointer - 4] == 13) {
                                break;
                            }
                        }
                        buffer = Arrays.copyOf(buffer, pointer);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));
                        String line = reader.readLine();
                        if (line == null || pointer < 4) {
                            if (socket.isClosed() || socket.getInputStream().available() == 0) {
                                socket.close();
                                break;
                            }
                            sendError(socket, 400);
                            continue;
                        }
                        String[] input = line.split(" ");
                        Map<String, String> headers = new HashMap<>();
                        while (true) {
                            line = reader.readLine();
                            if (line == null || line.isEmpty() || line.equals("\r") || line.equals("\n") || line.split(":").length < 2) {
                                break;
                            }
                            headers.put(line.split(":")[0].toLowerCase(), line.split(":")[1].substring(1));
                        }
                        if (headers.get("host") == null || headers.get("host").isEmpty()) {
                            sendError(socket, 400);
                            continue;
                        } else if (input.length != 3) {
                            sendError(socket, 400);
                            continue;
                        } else if (!input[2].equals("HTTP/1.1")) {
                            sendError(socket, 505);
                            continue;
                        }
                        if (headers.containsKey("cookie")) {
                            String[] cookies = headers.get("cookie").split(";");
                            for (String cookie : cookies) {
                                if (cookie.contains("SessionID")) {
                                    String[] parts = cookie.split("=");
                                    if (parts.length == 2) {
                                        if (instance.sessions.containsKey(parts[1])) {
                                            SessionImpl session = (SessionImpl) instance.sessions.get(parts[1]);
                                            session.lastAccessedTime = System.currentTimeMillis();
                                        }
                                    }
                                }
                            }
                        }
                        pointer = 0;
                        byte[] content = new byte[headers.get("content-length") == null ? 0 : Integer.parseInt(headers.get("content-length"))];
                        while (pointer < content.length) {
                            byte b = (byte) socket.getInputStream().read();
                            content[pointer++] = b;
                        }
                        switch (input[0]) {
                            case "GET":
                                getRequest(socket, input[1], headers, content, false);
                                break;
                            case "HEAD":
                                getRequest(socket, input[1], headers, content, true);
                                break;
                            case "POST":
                                postRequest(socket, input[1], headers, content);
                                break;
                            case "PUT":
                                putRequest(socket, input[1], headers, content);
                                break;
                            default:
                                sendError(socket, 501);
                                break;
                        }
                    }
                    socket.close();
                } catch (IOException e) {
                    System.err.println("Error with connection");
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    class SecureServerThread implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Socket socketTLS = serverSocketTLS.accept();
                    if (socketTLS == null) {
                        continue;
                    }
                    queue.put(socketTLS);
                } catch (IOException e) {
                    System.err.println("Could not listen on port " + instance.sPort);
                    System.out.println(e.getMessage());
                    System.exit(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    class ServerThread implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i < NUM_WORKER_THREADS; i++) {
                Thread worker = new Thread(new Responder());
                worker.start();
            }
            Thread reaper = new Thread(new SessionReaper());
            reaper.start();
            try {
                if (instance.sPort != -1) {
                    try {
                        String pwd = "secret";
                        KeyStore keyStore = KeyStore.getInstance("JKS");
                        keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
                        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                        keyManagerFactory.init(keyStore, pwd.toCharArray());
                        SSLContext sslContext = SSLContext.getInstance("TLS");
                        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                        ServerSocketFactory factory = sslContext.getServerSocketFactory();
                        serverSocketTLS = factory.createServerSocket(instance.sPort);
                        System.out.println("Secure server is listening on port " + instance.sPort);
                        logger.info("Secure server is listening on port " + instance.sPort);
                    } catch (Exception e) {
                        System.err.println("Could not create TLS socket:");
                        System.out.println(e.getMessage());
                    }
                }
                serverSocket = new ServerSocket(instance.port);
                System.out.println("Server is listening on port " + instance.port);
                logger.info("Server is listening on port " + instance.port);
                while (true) {
                    if (serverSocketTLS != null) {
                        Thread secureServerThread = new Thread(instance.new SecureServerThread());
                        secureServerThread.start();
                    }
                    Socket socket = serverSocket.accept();
                    if (socket == null) {
                        continue;
                    }
                    queue.put(socket);
                }
            } catch (IOException e) {
                System.err.println("Could not listen on port " + instance.port);
                System.out.println(e.getMessage());
                System.exit(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class staticFiles {
        public static void location(String loc) {
            if (instance == null) {
                instance = new Server();
            }
            instance.dir = loc;
        }
    }

    public class ServerRoute {
        Route handler;
        String method;

        public ServerRoute(Route r, String m) {
            handler = r;
            method = m;
        }
    }

    public static void securePort(int port) {
        if (instance == null) {
            instance = new Server();
        }
        if (!flag) {
            instance.sPort = port;
        }
    }

    public static void get(String p, Route r) {
        if (instance == null) {
            instance = new Server();
        }
        if (!flag) {
            flag = true;
            Thread serverThread = new Thread(instance.new ServerThread());
            serverThread.start();
        }
        routingTable.putIfAbsent(p, new ArrayList<>());
        routingTable.get(p).add(instance.new ServerRoute(r, "GET"));
    }

    public static void post(String p, Route r) {
        if (instance == null) {
            instance = new Server();
        }
        if (!flag) {
            flag = true;
            Thread serverThread = new Thread(instance.new ServerThread());
            serverThread.start();
        }
        routingTable.putIfAbsent(p, new ArrayList<>());
        routingTable.get(p).add(instance.new ServerRoute(r, "POST"));
    }

    public static void put(String p, Route r) {
        if (instance == null) {
            instance = new Server();
        }
        if (!flag) {
            flag = true;
            Thread serverThread = new Thread(instance.new ServerThread());
            serverThread.start();
        }
        routingTable.putIfAbsent(p, new ArrayList<>());
        routingTable.get(p).add(instance.new ServerRoute(r, "PUT"));
    }

    public static void port(int p) {
        if (instance == null) {
            instance = new Server();
        }
        instance.port = p;
    }

    private void sendError(Socket socket, int code) {
        System.out.println("Sending error " + code + " response");
        try {
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println("HTTP/1.1 " + code + " OK\r");
            writer.println("Content-Type: text/plain\r");
            writer.println("Content-Length: " + (code == 400 ? 11 : code == 403 ? 9 : code == 404 ? 9 : code == 405 ? 21 : code == 501 ? 15 : code == 505 ? 25 : 13) + "\r\n\r");
            writer.println("Error " + code + " " + (code == 400 ? "Bad Request" : code == 403 ? "Forbidden" : code == 404 ? "Not Found" : code == 405 ? "Method Not Allowed" : code == 501 ? "Not Implemented" : code == 505 ? "HTTP Version Not Supported" : "Unknown Error"));
        } catch (IOException e) {
            System.err.println("Error with sending error + " + code + " response");
            System.out.println(e.getMessage());
        }
    }

    private void getRequest(Socket socket, String path, Map<String, String> headers, byte[] content, boolean isHead) {
        try {
            if (!isHead) { //routingTable will never have HEAD requests
                HashMap<String, String> qParams = new HashMap<>();
                if (path.contains("?")) {
                    String[] parts = path.split("\\?");
                    path = parts[0];
                    parts = parts[1].split("&");
                    for (String param : parts) {
                        qParams.put(URLDecoder.decode(param.split("=")[0]), URLDecoder.decode(param.split("=")[1]));
                    }
                }
                if (headers.containsKey("content-type") && headers.get("content-type").equals("application/x-www-form-urlencoded")) {
                    String[] parts = new String(content).split("&");
                    for (String param : parts) {
                        qParams.put(URLDecoder.decode(param.split("=")[0]), URLDecoder.decode(param.split("=")[1]));
                    }
                }
                if (routingTable.containsKey(path)) {
                    for (ServerRoute route : routingTable.get(path)) {
                        if (route.method.equals("GET")) {
                            RequestImpl req = new RequestImpl("GET", path, "HTTP/1.1", headers, qParams, qParams, (InetSocketAddress) socket.getRemoteSocketAddress(), content, instance);
                            sendResponse(socket, route, req);
                            return;
                        }
                    }
                } else {
                    outer:
                    for (String cPath : routingTable.keySet()) {
                        if (cPath.contains(":")) {
                            String[] pathParts = path.split("/");
                            String[] cPathParts = cPath.split("/");
                            HashMap<String, String> params = new HashMap<>();
                            if (pathParts.length == cPathParts.length) {
                                for (int i = 0; i < pathParts.length; i++) {
                                    if (cPathParts[i].startsWith(":")) {
                                        params.put(cPathParts[i].substring(1), pathParts[i]);
                                    } else if (!cPathParts[i].equals(pathParts[i])) {
                                        continue outer;
                                    }
                                }
                                for (ServerRoute route : routingTable.get(cPath)) {
                                    if (route.method.equals("GET")) {
                                        RequestImpl req = new RequestImpl("GET", path, "HTTP/1.1", headers, qParams, params, (InetSocketAddress) socket.getRemoteSocketAddress(), new byte[0], instance);
                                        sendResponse(socket, route, req);
                                    }
                                }
                                return;
                            }
                        }
                    }
                }
            }
            File file = new File(dir + path);
            if (file.exists()) {
                if (!file.canRead() || file.getPath().contains("..")) {
                    sendError(socket, 403);
                    return;
                }
                if (headers.containsKey("if-modified-since")) {
                    Date modified = Date.from(Files.getLastModifiedTime(file.toPath()).toInstant());
                    Date since = Date.from(Instant.parse(headers.get("if-modified-since")));

                    if (modified.before(since)) {
                        sendError(socket, 304);
                        return;
                    }
                }
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                writer.println("HTTP/1.1 200 OK\r");
                if (path.endsWith(".jpg") || path.endsWith(".jpeg")) {
                    writer.println("Content-Type: image/jpeg\r");
                } else if (path.endsWith(".txt")) {
                    writer.println("Content-Type: text/plain\r");
                } else if (path.endsWith(".html")) {
                    writer.println("Content-Type: text/html\r");
                } else {
                    writer.println("Content-Type: application/octet-stream\r");
                }
                writer.println("Content-Length: " + file.length() + "\r");
                if (headers.containsKey("if-modified-since")) {
                    writer.println("Last-Modified: " + Files.getLastModifiedTime(file.toPath()) + "\r");
                }
                if (headers.containsKey("range")) {
                    String value = headers.get("range").split("=")[1];
                    if (value.startsWith("-")) {
                        writer.println("Content-Range: 0" + value + "/" + file.length() + "\r\n\r");
                        socket.getOutputStream().write(Files.readAllBytes(file.toPath()), 0, Integer.parseInt(value.substring(1)));
                    } else if (value.endsWith("-")) {
                        writer.println("Content-Range: " + value + (file.length() - 1) + "/" + file.length() + "\r\n\r");
                        socket.getOutputStream().write(Files.readAllBytes(file.toPath()), Integer.parseInt(value.substring(0, value.length() - 1)), (int) file.length() - 1);
                    } else {
                        String[] range = value.split("-");
                        int beginning = Integer.parseInt(range[0]);
                        int end = Integer.parseInt(range[1]);
                        writer.println("Content-Range: bytes " + beginning + "-" + end + "/" + file.length() + "\r\n\r");
                        socket.getOutputStream().write(Files.readAllBytes(file.toPath()), beginning, end - beginning + 1);
                    }
                } else if (isHead) {
                    writer.println("\r");
                } else {
                    writer.println("\r");
                    socket.getOutputStream().write(Files.readAllBytes(file.toPath()));
                }
            } else {
                sendError(socket, 404);
            }
        } catch (IOException e) {
            System.err.println("Error with GET request");
            System.out.println(e.getMessage());
        } catch (Exception e) {
            sendError(socket, 500);
            throw new RuntimeException(e);
        }
    }

    private void postRequest(Socket socket, String path, Map<String, String> headers, byte[] content) {
        try {
            HashMap<String, String> qParams = new HashMap<>();
            if (path.contains("?")) {
                String[] parts = path.split("\\?");
                path = parts[0];
                parts = parts[1].split("&");
                for (String param : parts) {
                    qParams.put(URLDecoder.decode(param.split("=")[0]), URLDecoder.decode(param.split("=")[1]));
                }
            }
            if (headers.containsKey("content-type") && headers.get("content-type").equals("application/x-www-form-urlencoded")) {
                String[] parts = new String(content).split("&");
                for (String param : parts) {
                    qParams.put(URLDecoder.decode(param.split("=")[0]), URLDecoder.decode(param.split("=")[1]));
                }
            }
            if (routingTable.containsKey(path)) {
                for (ServerRoute route : routingTable.get(path)) {
                    if (route.method.equals("POST")) {
                        RequestImpl req = new RequestImpl("POST", path, "HTTP/1.1", headers, qParams, null, (InetSocketAddress) socket.getRemoteSocketAddress(), content, instance);
                        sendResponse(socket, route, req);
                        return;
                    }
                }
            } else {
                outer:
                for (String cPath : routingTable.keySet()) {
                    if (cPath.contains(":")) {
                        String[] pathParts = path.split("/");
                        String[] cPathParts = cPath.split("/");
                        HashMap<String, String> params = new HashMap<>();
                        if (pathParts.length == cPathParts.length) {
                            for (int i = 0; i < pathParts.length; i++) {
                                if (cPathParts[i].startsWith(":")) {
                                    params.put(cPathParts[i].substring(1), pathParts[i]);
                                }
                                if (!cPathParts[i].equals(pathParts[i])) {
                                    continue outer;
                                }
                            }
                            for (ServerRoute route : routingTable.get(cPath)) {
                                if (route.method.equals("POST")) {
                                    RequestImpl req = new RequestImpl("POST", path, "HTTP/1.1", headers, qParams, params, (InetSocketAddress) socket.getRemoteSocketAddress(), new byte[0], instance);
                                    sendResponse(socket, route, req);
                                }
                            }
                            return;
                        }
                    }
                }
            }
            sendError(socket, 405);
        } catch (IOException e) {
            System.err.println("Error with POST request");
            System.out.println(e.getMessage());
        } catch (Exception e) {
            sendError(socket, 500);
            throw new RuntimeException(e);
        }
    }

    private void putRequest(Socket socket, String path, Map<String, String> headers, byte[] content) {
        try {
            HashMap<String, String> qParams = new HashMap<>();
            if (path.contains("?")) {
                String[] parts = path.split("\\?");
                path = parts[0];
                parts = parts[1].split("&");
                for (String param : parts) {
                    qParams.put(URLDecoder.decode(param.split("=")[0]), URLDecoder.decode(param.split("=")[1]));
                }
            }
            if (headers.containsKey("content-type") && headers.get("content-type").equals("application/x-www-form-urlencoded")) {
                String[] parts = new String(content).split("&");
                for (String param : parts) {
                    qParams.put(URLDecoder.decode(param.split("=")[0]), URLDecoder.decode(param.split("=")[1]));
                }
            }
            if (routingTable.containsKey(path)) {
                for (ServerRoute route : routingTable.get(path)) {
                    if (route.method.equals("PUT")) {
                        RequestImpl req = new RequestImpl("PUT", path, "HTTP/1.1", headers, qParams, null, (InetSocketAddress) socket.getRemoteSocketAddress(), content, instance);
                        sendResponse(socket, route, req);
                        return;
                    }
                }
            } else {
                outer:
                for (String cPath : routingTable.keySet()) {
                    if (cPath.contains(":")) {
                        String[] pathParts = path.split("/");
                        String[] cPathParts = cPath.split("/");
                        HashMap<String, String> params = new HashMap<>();
                        if (pathParts.length == cPathParts.length) {
                            for (int i = 0; i < pathParts.length; i++) {
                                if (cPathParts[i].startsWith(":")) {
                                    params.put(cPathParts[i].substring(1), pathParts[i]);
                                } else if (!cPathParts[i].equals(pathParts[i])) {
                                    continue outer;
                                }
                            }
                            for (ServerRoute route : routingTable.get(cPath)) {
                                if (route.method.equals("PUT")) {
                                    RequestImpl req = new RequestImpl("PUT", path, "HTTP/1.1", headers, qParams, params, (InetSocketAddress) socket.getRemoteSocketAddress(), new byte[0], instance);
                                    sendResponse(socket, route, req);
                                }
                            }
                            return;
                        }
                    }
                }
            }
            sendError(socket, 405);
        } catch (IOException e) {
            System.err.println("Error with PUT request");
            System.out.println(e.getMessage());
        } catch (Exception e) {
            sendError(socket, 500);
            throw new RuntimeException(e);
        }
    }

    private void sendResponse(Socket socket, ServerRoute route, RequestImpl req) throws IOException {
        ResponseImpl res = new ResponseImpl(socket);
        try {
            res.body((String) route.handler.handle(req, res));
            if (res.writeCalled) {
                socket.close();
                return;
            }

            if (req.createdSession) {
                res.headers.put("Set-Cookie", Collections.singletonList("SessionID=" + req.sessionId));
            }
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println("HTTP/1.1 " + res.statusCode + " " + res.reasonPhrase + "\r");
            writer.println("Content-Type: " + res.type + "\r");
            for (String key : res.headers.keySet()) {
                for (String value : res.headers.get(key)) {
                    writer.println(key + ": " + value + "\r");
                }
            }
            writer.println("Content-Length: " + res.body.length + "\r\n\r");
            if (res.body != null) {
                socket.getOutputStream().write(res.body);
            }
        } catch (Exception e) {
            if (res.writeCalled) {
                socket.close();
            } else {
                sendError(socket, 500);
                throw new RuntimeException(e);
            }
        }
    }
}