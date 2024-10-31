package cis5550.webserver;

import java.util.concurrent.ConcurrentHashMap;

public class SessionImpl implements Session {
    ConcurrentHashMap<String, Object> cookies = new ConcurrentHashMap<>();
    String id;
    long creationTime, lastAccessedTime;
    int maxActiveInterval = 300;
    boolean valid = true;

    public String id() {
        if (!valid) return null;
        lastAccessedTime = System.currentTimeMillis();
        return id;
    }

    public long creationTime() {
        if (!valid) return -1;
        lastAccessedTime = System.currentTimeMillis();
        return creationTime;
    }

    public long lastAccessedTime() {
        if (!valid) return -1;
        lastAccessedTime = System.currentTimeMillis();
        return lastAccessedTime;
    }

    public void maxActiveInterval(int seconds) {
        if (!valid) return;
        lastAccessedTime = System.currentTimeMillis();
        maxActiveInterval = seconds;
    }

    public void invalidate() {
        valid = false;
    }

    public Object attribute(String name) {
        if (!valid) return null;
        lastAccessedTime = System.currentTimeMillis();
        return cookies.get(name);
    }

    public void attribute(String name, Object value) {
        if (!valid) return;
        lastAccessedTime = System.currentTimeMillis();
        cookies.put(name, value);
    }
}
