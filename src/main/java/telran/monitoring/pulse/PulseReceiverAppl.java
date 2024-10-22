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

private static final int MAX_THRESHOLD_PULSE_VALUE = 210;
private static final int MIN_THRESHOLD_PULSE_VALUE = 40;
private static final int WARN_MAX_PULSE_VALUE = 180;
private static final int WARN_MIN_PULSE_VALUE = 55;

static DatagramSocket socket;
static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
static DynamoDB dynamo = new DynamoDB(client);
static Table table = dynamo.getTable("pulse_values");

static Logger logger = Logger.getLogger("LoggerAppl");

	public static void main(String[] args) throws Exception{
		LogManager.getLogManager().reset();
		Level level = getLevel(); 
		logger.setLevel(level);
		Handler handlerFile = new FileHandler("logs");
		handlerFile.setLevel(Level.FINEST);
		handlerFile.setFormatter(new SimpleFormatter());
		logger.addHandler(handlerFile);
		logger.info("DB Table name: " + table.getTableName());
		logger.config("Environment variables: " 
				+ "LOGGING_LEVEL: " + level + "; "
				+ "MAX_THRESHOLD_PULSE_VALUE: "+ MAX_THRESHOLD_PULSE_VALUE  + "; "
				+ "MIN_THRESHOLD_PULSE_VALUE: "+ MIN_THRESHOLD_PULSE_VALUE + "; "
				+ "WARN_MAX_PULSE_VALUE: "+ WARN_MAX_PULSE_VALUE + "; "
				+ "WARN_MIN_PULSE_VALUE: "+ WARN_MIN_PULSE_VALUE + "; ");
		
		socket  = new DatagramSocket(PORT);
		byte [] buffer = new byte[MAX_BUFFER_SIZE];
		while(true) {
			DatagramPacket packet = new DatagramPacket(buffer, MAX_BUFFER_SIZE);
			socket.receive(packet);
			processReceivedData(packet);
		}

	}
	private static Level getLevel() {
		Level level = Level.INFO;
		try {
			level = Level.parse(System.getenv("LOGGING_LEVEL"));
			
		} catch (IllegalArgumentException e) {
			logger.warning("Illegal level value");
		} catch(NullPointerException e) {
			logger.warning("Level env var not provided");
		}
		return level;
	}
	
	private static void processReceivedData(DatagramPacket packet) {
		String json = new String(Arrays.copyOf(packet.getData(), packet.getLength()));
		System.out.println(json);
		logger.fine(json);
		table.putItem(new PutItemSpec().withItem(Item.fromJSON(json)));
		JSONObject jsonObj = new JSONObject(json);
		logger.finer(jsonObj.getLong("patientId") + " " + jsonObj.getLong("timestamp"));
		int pulseValue = jsonObj.getInt("value");
		if((pulseValue > WARN_MAX_PULSE_VALUE & pulseValue <= MAX_THRESHOLD_PULSE_VALUE)
				|| (pulseValue < WARN_MIN_PULSE_VALUE & pulseValue >= MIN_THRESHOLD_PULSE_VALUE)) {
			logger.warning(json);
		}
		if(pulseValue > MAX_THRESHOLD_PULSE_VALUE || pulseValue < MIN_THRESHOLD_PULSE_VALUE) {
			logger.severe(json);
		}

	}

}
