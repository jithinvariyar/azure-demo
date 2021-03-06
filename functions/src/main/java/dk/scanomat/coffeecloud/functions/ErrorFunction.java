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

public class ErrorFunction {
	private static final String url = System.getenv("DBUrl");
	private static final String user = System.getenv("DBuser");
	private static final String password = System.getenv("DBpass");

	private static final String sql = "INSERT INTO error(code, error, errorlowercase, sn, fw, time) values(?, ?, ?, ?, ?, ?)";

	@FunctionName("ErrorProcessFunction")
	public void serviceBusProcess(
			@ServiceBusQueueTrigger(name = "msg", queueName = "errorqueue", connection = "") String message,
			final ExecutionContext context) {
		String jsonString = message;
		JSONObject jsonObject = new JSONObject(jsonString);
		parseJsonAndStoreInDB(jsonObject);
	}

	public static void parseJsonAndStoreInDB(JSONObject jsonObject) {
		int code = (int) jsonObject.getNumber("code");
		String error = jsonObject.getString("error");
		String errorLowerCase = error.toLowerCase();
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

			preparedStatement.setInt(1, code);
			preparedStatement.setString(2, error);
			preparedStatement.setString(3, errorLowerCase);
			preparedStatement.setString(4, sn);
			preparedStatement.setInt(5, fw);
			preparedStatement.setTimestamp(6, time);
			// sends the statement to the database server
			preparedStatement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
