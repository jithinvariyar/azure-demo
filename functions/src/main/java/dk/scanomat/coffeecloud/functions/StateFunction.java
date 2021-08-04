package dk.scanomat.coffeecloud.functions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.json.JSONObject;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.ServiceBusQueueTrigger;

public class StateFunction {
	private static final String url = System.getenv("DBUrl");
	private static final String user = System.getenv("DBuser");
	private static final String password = System.getenv("DBpass");
	
	private static final String sql = "INSERT INTO states (m, sn, fw, time) values (?, ?, ?, ?)";

	@FunctionName("StateProcessFunction")
	public void serviceBusProcess(
			@ServiceBusQueueTrigger(name = "msg", queueName = "statequeue", connection = "") String message,
			final ExecutionContext context) {
		String jsonString = message;
		JSONObject jsonObject = new JSONObject(jsonString);
		parseJsonAndStoreInDB(jsonObject);
	}

	public static void parseJsonAndStoreInDB(JSONObject jsonObject) {
		String m = jsonObject.getString("m");
		String sn = jsonObject.getJSONObject("Origin").getString("SN");
		int fw = (int) jsonObject.getJSONObject("Origin").getNumber("FW");
		long milliSeconds = jsonObject.getJSONObject("timestamp").getLong("milliseconds");
		Timestamp time = new Timestamp(milliSeconds);

		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try (Connection connection = DriverManager.getConnection(url, user, password);
				// Step 2:Create a statement using connection object
				PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

			preparedStatement.setString(1, m);
			preparedStatement.setString(2, sn);
			preparedStatement.setInt(3, fw);
			preparedStatement.setTimestamp(4, time);
			// sends the statement to the database server
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
