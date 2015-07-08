package de.tub.ise.aec.group5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.kit.aifb.dbe.hermes.Sender;

public class Helper {

	/** Logger object */
	private static final Logger log = Logger.getLogger(Logger.class);

	/**
	 * Find out under which ports and IP addresses the nodes operate using
	 * "NodesMapping.csv".
	 *
	 * @return the mapping with already constructed senders.
	 */
	public static HashMap<String, Sender> parseMapping() {
		PropertyConfigurator.configure("log4j.properties");
		
		HashMap<String, Sender> map = new HashMap<String, Sender>();
		String file = "src/main/resources/NodesMapping.csv";

		BufferedReader br = null;
		String line = "";

		try {
			br = new BufferedReader(new FileReader(file));

			/** Go over each line */
			while ((line = br.readLine()) != null) {
				String[] val = line.split(",");
				log.debug("ID: " + val[0] + " Host: " + val[1] + " Port: " + Integer.parseInt(val[2]));
				map.put(val[0], new Sender(val[1], Integer.parseInt(val[2])));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

			/** Clean up */
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {}
			}
		}

		return map;
	}

	public static HashMap<String, String> parseMappingInverse() {
		PropertyConfigurator.configure("log4j.properties");
		
		HashMap<String, String> map = new HashMap<String, String>();
		String file = "resources/NodesMapping.csv";

		BufferedReader br = null;
		String line = "";

		try {
			br = new BufferedReader(new FileReader(file));

			/** Go over each line */
			while ((line = br.readLine()) != null) {
				String[] val = line.split(",");
				log.debug("ID: " + val[0] + " Host: " + val[1] + " Port: " + Integer.parseInt(val[2]));
				map.put(val[1] + ":" + val[2], val[0]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

			/** Clean up */
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {}
			}
		}

		return map;
	}

	public static HashMap<String, HashSet<ReplicationTarget>> getReplicationTargets(String nodeID)
			throws ParserConfigurationException, SAXException, IOException {

		HashMap<String, HashSet<ReplicationTarget>> replicationTargets = new HashMap<String, HashSet<ReplicationTarget>>();

		/** Create DOM structure */
		DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
		builderFactory.setNamespaceAware(true);
		builderFactory.setIgnoringElementContentWhitespace(true);

		DocumentBuilder builder = null;
		builder = builderFactory.newDocumentBuilder();
		Document xmlDoc = builder.parse(new File("resources/ReplicationPathConfiguration.xml"));

		/** Get all <path> elements */
		NodeList list = xmlDoc.getElementsByTagName("path");

		/** Iterate over each <path> element */
		for (int i = 0; i < list.getLength(); i++) {
			HashSet<ReplicationTarget> set = new HashSet<ReplicationTarget>();
			/** Get the <path> element */
			Element path = (Element) list.item(i);
			
			/** Get the "start" attribute for that <path> element */
			String startPath = path.getAttribute("start");
			log.debug("Path Element #" + i + " - StartPath: " + startPath);
			
			/** Get all <link> elements for that <path> element */
			NodeList childNodes = path.getElementsByTagName("link");
			
			ReplicationTarget syncTarget = new ReplicationTarget("sync");
			ReplicationTarget asyncTarget = new ReplicationTarget("async");
			ReplicationTarget quorumTarget = new ReplicationTarget("quorum");
			
			/** Iterate over the <link> elements */
			for (int j = 0; j < childNodes.getLength(); j++) {
				/** Get the <link> element */
				Element link = (Element) childNodes.item(j);
				
				/** Get the "src" attribute for that <link> element */
				String src = link.getAttribute("src");
				log.debug("Link Element #" + j + " - Source: " + src);
				
				/** Only consider if "src" matches provided nodeID */
				if (src.equals(nodeID)) {
					String type = link.getAttribute("type");
					
					/** Checking whether it's async, sync, or quorum */
					switch (type) {
						case "sync":
							log.debug("Type: " + type + " for Link Element #" + j + " and StartPath " + startPath
									+ " -> Target: " + link.getAttribute("target"));
							
							/**
							 * If a sync type for the provided nodeID as "src"
							 * was found, at the target to the target list.
							 */
							syncTarget.addTarget(link.getAttribute("target"));
							break;
						
						case "async":
							log.debug("Type: " + type + " for Link Element #" + j + " and StartPath " + startPath
									+ " -> Target: " + link.getAttribute("target"));
							
							/**
							 * If an async type for the provided nodeID as "src"
							 * was found, at the target to the target list.
							 */
							asyncTarget.addTarget(link.getAttribute("target"));
							break;
						case "quorum":
							log.debug("Type: " + type + " for Link Element #" + j + " and StartPath " + startPath
									+ " -> Quorum Size: " + link.getAttribute("qsize"));
							
							/**
							 * If "src" is nodeID and replication strategy is a
							 * "quorum", first set the quorum size.
							 */
							quorumTarget.setQsize(Integer.parseInt(link.getAttribute("qsize")));
							
							/** Get all quorum participants */
							NodeList qParticipants = link.getElementsByTagName("qparticipant");
							
							/** Iterate over the particpants */
							for (int k = 0; k < qParticipants.getLength(); k++) {
								/** Get the participant... */
								Element qParticipant = (Element) qParticipants.item(k);
								
								/** ...and add it to the quorum target list. */
								quorumTarget.addTarget(qParticipant.getAttribute("name"));
								
								log.debug("Type: " + type + " for Link Element #" + j + " and StartPath " + startPath
										+ " -> Qparticipant: " + qParticipant.getAttribute("name"));
							}
							break;
					}
				}
			}
			
			/**
			 * Only if there are targets that need synchronous replication, add
			 * it to the set.
			 */
			if (syncTarget.getTargetSize() > 0) {
				log.debug("Sync: Adding to set");
				set.add(syncTarget);
			}
			
			/**
			 * Only if there are targets that need asynchronous replication, add
			 * it to the set.
			 */
			if (asyncTarget.getTargetSize() > 0) {
				log.debug("aSync: Adding to set");
				set.add(asyncTarget);
			}
			
			/**
			 * Only if there are sources that require a quorum to replicate, add
			 * it to the set.
			 */
			if (quorumTarget.getTargetSize() > 0) {
				log.debug("Quorum: Adding to set");
				set.add(quorumTarget);
			}
			
			/**
			 * Finally, add the <path> element (containing the provided nodeID
			 * as a source) to the map.
			 */
			if (!set.isEmpty()) {
				log.debug("Adding to Map");
				replicationTargets.put(startPath, set);
			}
		}
		
		log.info("----------------------");
		return replicationTargets;
	}
}
