package de.tub.ise.aec.group5;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.opencsv.CSVWriter;

import edu.kit.aifb.dbe.hermes.Receiver;
import edu.kit.aifb.dbe.hermes.RequestHandlerRegistry;
import edu.kit.aifb.dbe.hermes.SimpleFileLogger;

public class TestClient {
	
	private static Logger logger = Logger.getLogger(TestClient.class);

	static int nbTests = 100;
	static byte[] value = new byte[100];
	private static RequestHandlerRegistry req = null;
	private static double writeTimeA;
	private static double writeTimeB;
	private static double writeTimeC;
	private static double writeTimeFinal;

	static {
		new Random().nextBytes(value);
	}

	public static void main(String[] args)
			throws IOException, InterruptedException {
		PropertyConfigurator.configure("log4j.properties");
		SimpleFileLogger.getInstance();
		
		Client.getInstance(1000);

		// logger.info("---- Start Calculating latency  ----");
		// String csv = "resources/latency.csv"; // the file storing the latency
		// Client.create("52.18.103.8", 2000, "cloud", value);
		// Client.update("52.18.103.8", 2000, "cloud", value);
		// CSVWriter writer = new CSVWriter(new FileWriter(csv));
		// for (int i = 0; i < nbTests; i++) {
		// long startTime = System.currentTimeMillis();
		// logger.info("start time" + startTime);
		// Client.update("52.18.103.8", 2000, "cloud", value);
		// long endTime = System.currentTimeMillis();
		// double latency = endTime - startTime;
		// logger.info("latency is" + latency);
		// writer.writeNext(Double.toString(latency).split("#"));
		// Thread.sleep(5000);
		// }
		// writer.close();
		// logger.info("---- END latency calculation ");
		//
		TimeHandler time = new TimeHandler();
		int port = 6000;
		try {
			
			Receiver receiver = new Receiver(port, 3, 3);
			receiver.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		TestClient.getRequestHandlerRegistry().registerHandler("writeTime", time);
		
		logger.info("---- Start Calculating staleness  ----");
		String staleFile = "resources/sync-stalness.csv"; // the file where the
		// staleness
		Client.create("52.18.103.8", 2000, "tub", value);
		// Client.update("52.18.103.8", 2000, "tub", value);
		CSVWriter stale = new CSVWriter(new FileWriter(staleFile));
		for (int i = 0; i < nbTests; i++) {
			Client.update("52.18.103.8", 2000, "tub", value);
			long endTime = System.currentTimeMillis();
			logger.info("the read value  is" + Client.read("52.18.103.8", 2000, "tub"));
			writeTimeA = time.getWriteTimeA();
			writeTimeB = time.getWriteTimeB();
			writeTimeC = time.getWriteTimeC();
			writeTimeFinal = Math.max(Math.max(writeTimeA, writeTimeB), writeTimeC);
			logger.info(" time to write on A is" + writeTimeA);
			logger.info(" time to write on B is" + writeTimeB);
			logger.info(" time to write on C is" + writeTimeC);
			logger.info(" time to write on final replica is" + writeTimeFinal);
			double staleness = writeTimeFinal - endTime;
			stale.writeNext(Double.toString(staleness).split("#"));
			Thread.sleep(5000);
		}
		stale.close();
		logger.info("---- END staleness calculation ");
		
	}
	
	public static RequestHandlerRegistry getRequestHandlerRegistry() {
		if (req == null) {
			req = RequestHandlerRegistry.getInstance();
		}
		return req;
	}
	
}
