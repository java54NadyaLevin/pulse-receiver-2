package telran.monitoring.pulse;

import java.net.*;
import java.util.Arrays;
import java.util.logging.*;

import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.*;

public class PulseReceiverAppl {
	private static final int PORT = 5000;
	private static final int MAX_BUFFER_SIZE = 1500;
	private static final Level DEFAULT_LOGGING_LEVEL = Level.INFO;
	private static final int MAX_THRESHOLD_PULSE_VALUE = 210;
	private static final int MIN_THRESHOLD_PULSE_VALUE = 40;
	private static final int WARN_MAX_PULSE_VALUE = 180;
	private static final int WARN_MIN_PULSE_VALUE = 55;

	static DatagramSocket socket;
	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
	static DynamoDB dynamo = new DynamoDB(client);
	static Table table = dynamo.getTable("pulse_values");

	static Logger logger = Logger.getLogger("LoggerAppl");

	public static void main(String[] args) throws Exception {
		LogManager.getLogManager().reset();
		Level level = getLevel();
		logger.setLevel(level);
		Handler handlerConsole = new ConsoleHandler();
		handlerConsole.setFormatter(new SimpleFormatter());
		handlerConsole.setLevel(Level.FINEST);
		logger.addHandler(handlerConsole);

		logger.info("DB Table name: " + table.getTableName());
		logger.config("Environment variables: " + "LOGGING_LEVEL: " + level + "; " + "MAX_THRESHOLD_PULSE_VALUE: "
				+ MAX_THRESHOLD_PULSE_VALUE + "; " + "MIN_THRESHOLD_PULSE_VALUE: " + MIN_THRESHOLD_PULSE_VALUE + "; "
				+ "WARN_MAX_PULSE_VALUE: " + WARN_MAX_PULSE_VALUE + "; " + "WARN_MIN_PULSE_VALUE: "
				+ WARN_MIN_PULSE_VALUE + "; ");

		socket = new DatagramSocket(PORT);
		byte[] buffer = new byte[MAX_BUFFER_SIZE];
		while (true) {
			DatagramPacket packet = new DatagramPacket(buffer, MAX_BUFFER_SIZE);
			socket.receive(packet);
			processReceivedData(packet);
		}

	}

	private static Level getLevel() {
		Level level = DEFAULT_LOGGING_LEVEL;
		try {
			level = Level.parse(System.getenv("LOGGING_LEVEL"));

		} catch (IllegalArgumentException e) {
			logger.warning("Illegal level value");
		} catch (NullPointerException e) {
			logger.warning("Level env var not provided, default value is " + level);
		}
		return level;
	}

	private static void processReceivedData(DatagramPacket packet) {
		String json = new String(Arrays.copyOf(packet.getData(), packet.getLength()));
		JSONObject jsonObj = new JSONObject(json);
		logger.fine(json);
		if (logMessage(jsonObj)) {
			try {
				table.putItem(new PutItemSpec().withItem(Item.fromJSON(json)));
				logger.finer(
						"Patient ID: " + jsonObj.getLong("patientId") + " Timestamp: " + jsonObj.getLong("timestamp"));
			} catch (Exception e) {
				logger.warning("Item haven't been put to DB");
			}
		}

	}

	private static boolean logMessage(JSONObject jsonObj) {
		boolean res = true;
		int pulseValue = jsonObj.getInt("value");
		String json = jsonObj.toString();
		if (pulseValue > WARN_MAX_PULSE_VALUE & pulseValue <= MAX_THRESHOLD_PULSE_VALUE) {
			logger.warning("Critically high value " + json);
		} else if (pulseValue < WARN_MIN_PULSE_VALUE & pulseValue >= MIN_THRESHOLD_PULSE_VALUE) {
			logger.warning("Critically low value " + json);
		} else if (pulseValue > MAX_THRESHOLD_PULSE_VALUE) {
			logger.severe("Value is greater than the maximum threshold " + json);
			res = false;
		} else if (pulseValue < MIN_THRESHOLD_PULSE_VALUE) {
			logger.severe("Value is lower than the minimum threshold " + json);
			res = false;
		}
		return res;
	}

}
