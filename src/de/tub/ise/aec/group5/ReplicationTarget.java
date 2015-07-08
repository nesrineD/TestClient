package de.tub.ise.aec.group5;

import java.util.HashSet;

public class ReplicationTarget {

	/** Differentiation between 'sync', 'async', and 'quorum' */
	private String type;
	
	/** Set of targets that need to be addressed according to the type */
	private HashSet<String> targets;
	
	/** Only needed when replication strategy is following a quorum */
	private int qsize = 0;
	
	public ReplicationTarget(String type) {
		this.type = type;
		this.targets = new HashSet<String>();
	}
	
	/**
	 * How requests should be forwarded
	 * @return 'sync', 'async', or 'quorum'
	 */
	public String getType() {
		return this.type;
	}
	
	/**
	 * Add a new target to the set
	 * @param target
	 */
	public void addTarget(String target) {
		this.targets.add(target);
	}
	
	/**
	 * Get all targets.
	 * @return targets
	 */
	public HashSet<String> getTargets() {
		return this.targets;
	}
	
	/**
	 * How many targets should be addressed using the replication type.
	 * @return the number of targets
	 */
	public int getTargetSize() {
		return this.targets.size();
	}
	
	/**
	 * Set the amount of positive responses needed when using a quorum
	 * @param qsize
	 */
	public void setQsize(int qsize) {
		this.qsize = qsize;
	}
	
	/**
	 * How many responses are required for quorum to be successful.
	 * @return the quorum size
	 */
	public int getQsize() {
		return this.qsize;
	}
	
}
