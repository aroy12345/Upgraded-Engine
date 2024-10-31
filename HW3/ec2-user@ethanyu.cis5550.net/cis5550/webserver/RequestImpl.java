package cis5550.webserver;

import java.util.*;
import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
    String method;
    String url;
    String protocol;
    InetSocketAddress remoteAddr;
    Map<String,String> headers;
    Map<String,String> queryParams;
    Map<String,String> params;
    byte bodyRaw[];
    Server server;
    boolean createdSession = false;
    String sessionId = null;

    RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg) {
        method = methodArg;
        url = urlArg;
        remoteAddr = remoteAddrArg;
        protocol = protocolArg;
        headers = headersArg;
        queryParams = queryParamsArg;
        params = paramsArg;
        bodyRaw = bodyRawArg;
        server = serverArg;
    }

    public String requestMethod() {
        return method;
    }
    public void setParams(Map<String,String> paramsArg) {
        params = paramsArg;
    }
    public int port() {
        return remoteAddr.getPort();
    }
    public String url() {
        return url;
    }
    public String protocol() {
        return protocol;
    }
    public String contentType() {
        return headers.get("content-type");
    }
    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }
    public String body() {
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }
    public byte[] bodyAsBytes() {
        return bodyRaw;
    }
    public int contentLength() {
        return bodyRaw.length;
    }
    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }
    public Set<String> headers() {
        return headers.keySet();
    }
    public String queryParams(String param) {
        return queryParams.get(param);
    }
    public Set<String> queryParams() {
        return queryParams.keySet();
    }
    public String params(String param) {
        return params.get(param);
    }
    public Map<String,String> params() {
        return params;
    }
    public Session session() {
        if (headers.get("cookie") != null) {
            String[] cookies = headers.get("cookie").split(";");
            for (String cookie : cookies) {
                if (cookie.contains("SessionID")) {
                    String[] cookieParts = cookie.split("=");
                    if (server.sessions.containsKey(cookieParts[1])) {
                        this.sessionId = cookieParts[1];
                        return server.sessions.get(cookieParts[1]);
                    }
                }
            }
        }
        SessionImpl newSession = new SessionImpl();
        String sessionID = UUID.randomUUID().toString();
        newSession.id = sessionID;
        newSession.creationTime = System.currentTimeMillis();
        newSession.lastAccessedTime = System.currentTimeMillis();
        server.sessions.put(sessionID, newSession);
        this.createdSession = true;
        this.sessionId = sessionID;
        return newSession;
    }

}
