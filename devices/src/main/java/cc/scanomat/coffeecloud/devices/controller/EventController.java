package cc.scanomat.coffeecloud.devices.controller;

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
		    Message msg = new Message(jsonString.getBytes());
		    msg.setProperty("type", "state");
		    client.sendEventAsync(msg, new DeviceStatusCallBack(), null);
		    Thread.sleep(5000);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return "Message send to IOT-Hub";
	}
	protected static class DeviceStatusCallBack implements IotHubEventCallback {
		public void execute(IotHubStatusCode responseStatus, Object callbackContext) {
			System.out.println("IoT Hub responded to device message with status " + responseStatus.name());
		}
	}
}
