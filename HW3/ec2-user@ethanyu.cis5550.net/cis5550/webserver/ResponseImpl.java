package cis5550.webserver;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.net.Socket;

public class ResponseImpl implements Response {

    int statusCode = 200;
    String type = "text/plain", reasonPhrase = "OK";
    byte[] body;
    Map<String, List<String>> headers = new java.util.HashMap<>();
    boolean writeCalled = false, bodyCalled = false, bodyAsBytesCalled = false;
    Socket socket;

    public ResponseImpl(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void body(String body) {
        if (writeCalled) {
            return;
        }
        bodyCalled = true;
        if (body == null) {
            this.body = null;
        } else {
            this.body = body.getBytes();
        }

    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if (writeCalled) {
            return;
        }
        bodyAsBytesCalled = true;
        this.body = bodyArg;
    }

    @Override
    public void header(String name, String value) {
        if (writeCalled) {
            return;
        }
        headers.putIfAbsent(name, new ArrayList<>());
        headers.get(name).add(value);
    }

    @Override
    public void type(String contentType) {
        if (writeCalled) {
            return;
        }
        type = contentType;
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (writeCalled) {
            return;
        }
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (!writeCalled) {
            writeCalled = true;
            headers.putIfAbsent("Connection", new ArrayList<>());
            headers.get("Connection").add("close");
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r");
            writer.println("Content-Type: " + type + "\r");
            for (String key : headers.keySet()) {
                for (String value : headers.get(key)) {
                    writer.println(key + ": " + value + "\r");
                }
            }
            writer.println("\r");
        }
        socket.getOutputStream().write(b);
    }

    @Override
    public void redirect(String url, int responseCode) {

    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {

    }
}
