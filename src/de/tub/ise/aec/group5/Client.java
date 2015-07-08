package de.tub.ise.aec.group5;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.kit.aifb.dbe.hermes.Request;
import edu.kit.aifb.dbe.hermes.Response;
import edu.kit.aifb.dbe.hermes.Sender;

public class Client {
	
	private static Logger logger = Logger.getLogger(Client.class);
	private static long timeout;
	
	private static final Client instance = new Client();

	private static HashMap<String, String> senders = Helper.parseMappingInverse();
	
	private Client() {}
	
	/**
	 * @return the singleton
	 */
	public static Client getInstance(long timeout) {
		Client.timeout = timeout;
		return instance;
	}
	
	private static Response sendMessage(String host, int port, Request req) {
		Sender s = new Sender(host, port);
		return s.sendMessage(req, timeout);
	}
	
	public static boolean create(String host, int port, String key, byte[] value) {
		Request req = new Request("create", senders.get(host + ":" + port));
		System.out.println(senders.get(host + ":" + port));
		req.addItem(key);
		req.addItem(value);
		Response resp = sendMessage(host, port, req);
		System.out.println(resp.getResponseMessage()
				+ ((resp.getItems().size() > 0) ? resp.getItems().get(0) : "empty"));
		return resp.responseCode();
	}

	public static byte[] read(String host, int port, String key) {
		Request req = new Request("read", senders.get(host + ":" + port));
		req.addItem(key);
		Response resp = sendMessage(host, port, req);
		if (resp.responseCode() && (resp.getItems().size() > 0)) {
			return (byte[]) resp.getItems().get(0);
		} else {
			return resp.getResponseMessage().getBytes();
		}
	}

	public static boolean update(String host, int port, String key, byte[] value) {
		Request req = new Request("update", senders.get(host + ":" + port));
		req.addItem(key);
		req.addItem(value);
		Response resp = sendMessage(host, port, req);
		logger.info("the response is sent   " + resp + "  resp code" + resp.responseCode());
		return resp.responseCode();
	}
	
	public static boolean delete(String host, int port, String key) {
		Request req = new Request("delete", senders.get(host + ":" + port));
		req.addItem(key);
		Response resp = sendMessage(host, port, req);
		return resp.responseCode();
	}
	
}
