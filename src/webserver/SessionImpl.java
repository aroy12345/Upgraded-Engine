package webserver;

import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session {

	private String sessionId;
	private long creationTime;
	private long lastAccessedTime;
	private int maxActiveInterval; // in seconds
	private Map<String, Object> attributes = new HashMap<>();

	public SessionImpl(String id) {
		this.sessionId = id;
		this.creationTime = System.currentTimeMillis();
		this.lastAccessedTime = creationTime;
		this.maxActiveInterval = 300; // Default, session never expires unless specified
	}

	@Override
	public String id() {
		return sessionId;
	}

	@Override
	public long creationTime() {
		return creationTime;
	}

	@Override
	public long lastAccessedTime() {
		return lastAccessedTime;
	}

	public int getMaxActiveInterval() {
		return maxActiveInterval;
	}

	public void setLastAccessedTime(long x) {
		this.lastAccessedTime = x;
	}

	@Override
	public void maxActiveInterval(int seconds) {
		this.maxActiveInterval = seconds;
	}

	@Override
	public void invalidate() {
		// Clear all attributes and invalidate the session
		attributes.clear();
	}

	@Override
	public Object attribute(String name) {
		this.lastAccessedTime = System.currentTimeMillis();
		return attributes.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		attributes.put(name, value);
		// Update last accessed time whenever an attribute is added
		this.lastAccessedTime = System.currentTimeMillis();
	}

	public boolean isExpired() {
		// If maxActiveInterval is -1, the session never expires.
		if (maxActiveInterval == -1) {
			return false;
		}
		// Calculate the current time minus the last accessed time and compare it to
		// maxActiveInterval.
		long elapsedSinceLastAccess = (System.currentTimeMillis() - lastAccessedTime) / 1000; // Convert to seconds
		return elapsedSinceLastAccess >= maxActiveInterval;
	}

}
