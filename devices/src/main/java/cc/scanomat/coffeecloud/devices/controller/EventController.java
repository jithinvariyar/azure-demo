package cc.scanomat.coffeecloud.devices.controller;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.microsoft.azure.sdk.iot.device.DeviceClient;
import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import com.microsoft.azure.sdk.iot.device.IotHubEventCallback;
import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.Message;

@RestController
@RequestMapping("/events")
public class EventController {
	private static String connString = "HostName=DemoCC.azure-devices.net;DeviceId=4800286;SharedAccessKey=prxIjqjZqHL82640LWbYIBjvZe2qFsvAtbXO4ipZblU=";
	private static IotHubClientProtocol protocol = IotHubClientProtocol.MQTT;
	
	@PostMapping("/order")
	public String sendOrder(@RequestBody String jsonString) {
		try(DeviceClient client = new DeviceClient(connString, protocol)) {
			client.open();
			JSONObject jsonObject = new JSONObject(jsonString);
			jsonObject = addCurrentTimestamp(jsonObject);
			jsonString = jsonObject.toString();
		    Message msg = new Message(jsonString.getBytes());
		    msg.setProperty("type", "order");
		    client.sendEventAsync(msg, new DeviceStatusCallBack(), null);
		    Thread.sleep(5000);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return "Message send to IOT-Hub";
	}
	
	@PostMapping("/error")
	public String sendError(@RequestBody String jsonString) {
		try(DeviceClient client = new DeviceClient(connString, protocol)) {
			client.open();
			JSONObject jsonObject = new JSONObject(jsonString);
			jsonObject = addCurrentTimestamp(jsonObject);
			jsonString = jsonObject.toString();
		    Message msg = new Message(jsonString.getBytes());
		    msg.setProperty("type", "error");
		    client.sendEventAsync(msg, new DeviceStatusCallBack(), null);
		    Thread.sleep(5000);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return "Message send to IOT-Hub";
	}
	
	@PostMapping("/state")
	public String sendState(@RequestBody String jsonString) {
		try(DeviceClient client = new DeviceClient(connString, protocol)) {
			client.open();
			JSONObject jsonObject = new JSONObject(jsonString);
			jsonObject = addCurrentTimestamp(jsonObject);
			jsonString = jsonObject.toString();
		    Message msg = new Message(jsonString.getBytes());
		    msg.setProperty("type", "state");
		    client.sendEventAsync(msg, new DeviceStatusCallBack(), null);
		    Thread.sleep(5000);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return "Message send to IOT-Hub";
	}
	
	private static JSONObject addCurrentTimestamp(JSONObject event) {
		Date date = new Date();
		DateFormat ymdDateFormat = new SimpleDateFormat("yyyyMMdd");
		DateFormat hourOfDayFormat = new SimpleDateFormat("HH");
		DateFormat dayOfWeekFormat = new SimpleDateFormat("u");
		DateFormat monthFormat = new SimpleDateFormat("MM");

		String yearMonthDay = ymdDateFormat.format(date);
		String hourOfDay = hourOfDayFormat.format(date);
		String dayOfWeek = dayOfWeekFormat.format(date);
		String month = monthFormat.format(date);

		Map<String, Object> dateMap = new HashMap<>();
		dateMap.put("full", date.toString());
		dateMap.put("milliseconds", date.getTime());
		dateMap.put("yearMonthDay", yearMonthDay);
		dateMap.put("hourOfDay", hourOfDay);
		dateMap.put("dayOfWeek", dayOfWeek);
		dateMap.put("month", month);

		event.put("timestamp", dateMap);
		return event;
	}
	
	protected static class DeviceStatusCallBack implements IotHubEventCallback {
		public void execute(IotHubStatusCode responseStatus, Object callbackContext) {
			System.out.println("IoT Hub responded to device message with status " + responseStatus.name());
		}
	}
}
