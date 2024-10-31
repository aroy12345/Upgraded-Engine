package webserver;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

// Provided as part of the framework code

class RequestImpl implements Request {
	String method;
	String url;
	String protocol;
	InetSocketAddress remoteAddr;
	Map<String, String> headers;
	Map<String, String> queryParams;
	Map<String, String> params;
	byte bodyRaw[];
	Server server;

	private SessionImpl sesh;
	private ResponseImpl response;
	private boolean newSesh;

	private int count = 0;

	RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String, String> headersArg,
			Map<String, String> queryParamsArg, Map<String, String> paramsArg, InetSocketAddress remoteAddrArg,
			byte bodyRawArg[], Server serverArg, SessionImpl S, ResponseImpl R, boolean b) {
		method = methodArg;
		url = urlArg;
		remoteAddr = remoteAddrArg;
		protocol = protocolArg;
		headers = headersArg;
		queryParams = queryParamsArg;
		params = paramsArg;
		bodyRaw = bodyRawArg;
		server = serverArg;
		sesh = S;
		response = R;
		newSesh = b;

	}

	@Override
	public String requestMethod() {
		return method;
	}

	public void setParams(Map<String, String> paramsArg) {
		params = paramsArg;
	}

	@Override
	public int port() {
		return remoteAddr.getPort();
	}

	@Override
	public String url() {
		return url;
	}

	@Override
	public String protocol() {
		return protocol;
	}

	@Override
	public String contentType() {
		return headers.get("content-type");
	}

	@Override
	public String ip() {
		return remoteAddr.getAddress().getHostAddress();
	}

	@Override
	public String body() {
		return new String(bodyRaw, StandardCharsets.UTF_8);
	}

	@Override
	public byte[] bodyAsBytes() {
		return bodyRaw;
	}

	@Override
	public int contentLength() {
		return bodyRaw.length;
	}

	@Override
	public String headers(String name) {
		return headers.get(name.toLowerCase());
	}

	@Override
	public Set<String> headers() {
		return headers.keySet();
	}

	@Override
	public String queryParams(String param) {
		if(queryParams == null) {
			return null;
		}
		return queryParams.get(param);
	}

	@Override
	public Set<String> queryParams() {
		return queryParams.keySet();
	}

	@Override
	public String params(String param) {
		return params.get(param);
	}

	@Override
	public Map<String, String> params() {
		return params;
	}

	@Override
	public Session session() {
		// TODO Auto-generated method stub
		if (newSesh && count == 0) {
			response.header("Set-Cookie", "SessionID=" + sesh.id());
			count++;
		}

		return sesh;

	}

}
