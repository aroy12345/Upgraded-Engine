package webserver;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ResponseImpl implements Response {

	private int statusCode = 200;
	private String reasonPhrase = "OK";
	private List<String> headers = new ArrayList<>();
	private byte[] body = null;
	private String stringBody = null;
	private boolean isCommitted = false;
	PrintWriter ot;
	OutputStream outputStr;
	private int write = 0;
	private String method;
	private String what;

	public int getWrite() {
		return write;
	}

	public ResponseImpl(PrintWriter o, OutputStream S, String a, String b) {
		ot = o;
		outputStr = S;
		method = a;
		what = b;
	}

	public boolean isCommitted() {
		return isCommitted;
	}

	public List<String> getHeaders() {
		return headers;
	}

	@Override
	public void body(String body) {

		if (!isCommitted) {
			stringBody = body;
			this.setBody(body.getBytes());
		}

	}

	public void setStringBody(String body) {
		stringBody = body;
		this.setBody(body.getBytes());
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		if (!isCommitted) {
			this.setBody(bodyArg);
		}
	}

	@Override
	public void header(String name, String value) {
		if (!isCommitted) {
			headers.add(name + ": " + value);
		}
	}

	@Override
	public void type(String contentType) {
		header("Content-Type", contentType);

	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		if (!isCommitted) {
			this.setStatusCode(statusCode);
			this.setReasonPhrase(reasonPhrase);
		}
	}

	@Override
	public void write(byte[] b) throws Exception {
		// isCommitted = true;
		if (write == 0) {
			String type = "text/html";
			if (method.equals("GET")) {
				type = "text/plain";
			}

			ot.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");
			ot.print("Content-Type: " + type + "\r\n");
			ot.print("Connection: close\r\n");

			for (String S : headers)
				ot.print(S + "\r\n");

			ot.print("\r\n");

			write++;
			ot.flush();

		}

		String s = new String(b, StandardCharsets.UTF_8);
		if (stringBody == null) {
			setStringBody(s);
		} else {
			setStringBody(stringBody + s);
		}

		outputStr.write(b);
		outputStr.flush();
		isCommitted = true;
	}

	@Override
	public void redirect(String url, int responseCode) {
		// TODO Auto-generated method stub

	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		// TODO Auto-generated method stub

	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public String getReasonPhrase() {
		return reasonPhrase;
	}

	public void setReasonPhrase(String reasonPhrase) {
		this.reasonPhrase = reasonPhrase;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

}
