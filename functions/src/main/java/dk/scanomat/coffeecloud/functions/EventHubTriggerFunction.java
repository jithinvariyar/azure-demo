package dk.scanomat.coffeecloud.functions;

import org.json.JSONException;
import org.json.JSONObject;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.SendGridOutput;

public class EventHubTriggerFunction {
	@FunctionName("ehprocessor")
	public void eventHubProcessor(
			@EventHubTrigger(name = "msg", eventHubName = "", connection = "eventhubConnString", cardinality = Cardinality.ONE) String eventHubMessage,
			@SendGridOutput(name = "message", dataType = "String", apiKey = "sendGridAPIKey", to = "jithin@mailinator.com", from = "jithin@vinnovatelabz.com", subject = "Event From The Machine", text = "Sent from Azure Functions") OutputBinding<String> message,
			final ExecutionContext context) {

		String jsonString = eventHubMessage;
		JSONObject jsonObject = new JSONObject(jsonString);
		final String toAddress = "jithin@mailinator.com";
		final String toAddressMail = "jishnu@mailinator.com";

		String eventType = null;
		String value = null;
		try {
			jsonObject.getString("product");
			eventType = "order";
		} catch (JSONException notOrder) {
			try {
				jsonObject.getNumber("code");
				eventType = "error";
			} catch (JSONException notError) {
				try {
					jsonObject.getString("m");
					eventType = "state";
				} catch (JSONException notState) {
					eventType = "unknown";
				}
			}
		}

		switch (eventType) {
		case "order":
			value = sendOrderEmail(jsonObject);
			break;
		case "error":
			value = sendErrorEmail(jsonObject);
			break;
		case "state":
			value = sendStateEmail(jsonObject);
			break;
		case "unknown":
			value = "An UNKNOWN event occured";
			break;
		}

		StringBuilder builder = new StringBuilder().append("{")
				.append("\"personalizations\": [{ \"to\": [{ \"email\": \"%s\"},{ \"email\": \"%s\"}]}],")
				.append("\"content\": [{\"type\": \"text/plain\", \"value\": \"%s\"}]").append("}");

		final String body = String.format(builder.toString(), toAddress, toAddressMail, value);

		message.setValue(body);
	}

	public static String sendOrderEmail(JSONObject jsonObject) {
		String product = jsonObject.getString("product");
		String sn = jsonObject.getJSONObject("Origin").getString("SN");

		String message = "You have an order\n" + product + "\nFrom the machine: " + sn;
		return message;
	}

	public static String sendErrorEmail(JSONObject jsonObject) {
		String error = jsonObject.getString("error");
		String sn = jsonObject.getJSONObject("Origin").getString("SN");

		String message = "An Error occured : " + error + "\nplease inspect the machine: " + sn;
		return message;
	}

	public static String sendStateEmail(JSONObject jsonObject) {
		String m = jsonObject.getString("m");
		String sn = jsonObject.getJSONObject("Origin").getString("SN");

		String message = "The current state of the machine: " + sn + "\n" + m;
		return message;
	}
}
