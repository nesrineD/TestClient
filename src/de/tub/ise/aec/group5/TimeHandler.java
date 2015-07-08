package de.tub.ise.aec.group5;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import edu.kit.aifb.dbe.hermes.IRequestHandler;
import edu.kit.aifb.dbe.hermes.Request;
import edu.kit.aifb.dbe.hermes.Response;
import edu.kit.aifb.dbe.hermes.Sender;

public class TimeHandler
		implements IRequestHandler {

	private static Logger logger = Logger.getLogger(TimeHandler.class);
	
	private double writeTimeA;
	private double writeTimeB;
	private double writeTimeC;

	/** Map of all senders */
	private HashMap<String, Sender> senders;

	/** Map of replication targets for the specific node */
	private HashMap<String, HashSet<ReplicationTarget>> replicationTargets;

	/**
	 * Constructor for the CreateHandler
	 *
	 * @param senders
	 * @param replicationTargets
	 */
	public TimeHandler() {

	}

	@Override
	public boolean hasPriority() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public boolean requiresResponse() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public Response handleRequest(Request req) {
		List<Serializable> items = new ArrayList<Serializable>();
		items = req.getItems();
		logger.info("  the originator is  " + req.getOriginator());
		if (req.getOriginator().equals("nodeA")) {
			setWriteTimeA((long) items.get(0));
			logger.info("the time to write on node A is" + getWriteTimeA());
			Response resp = new Response(writeTimeA, "the write time is :", true, req);
			return resp;
		} else if (req.getOriginator().equals("nodeB")) {
			setWriteTimeB((long) items.get(0));
			logger.info("the time to write on node B is" + getWriteTimeB());
			Response resp = new Response(writeTimeB, "the write time is :", true, req);
			return resp;

		} else {
			setWriteTimeC((long) items.get(0));
			logger.info("the time to write on node C is" + getWriteTimeC());
			Response resp = new Response(writeTimeC, "the write time is :", true, req);
			return resp;

		}
	}

	public double getWriteTimeA() {
		return writeTimeA;
	}

	public void setWriteTimeA(double writeTimeA) {
		this.writeTimeA = writeTimeA;
	}

	public double getWriteTimeB() {
		return writeTimeB;
	}

	public void setWriteTimeB(double writeTimeB) {
		this.writeTimeB = writeTimeB;
	}

	public double getWriteTimeC() {
		return writeTimeC;
	}

	public void setWriteTimeC(double writeTimeC) {
		this.writeTimeC = writeTimeC;
	}
}
