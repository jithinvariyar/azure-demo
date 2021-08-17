package dk.scanomat.coffeecloud.functions;

import java.util.Collections;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

public class CosmosDbFunction {

	private CosmosClient client;
	private final String databaseName = "coffeecloud_db";
	private final String orderContainerName = "orderContainer";
	private final String errorContainerName = "errorContainer";
	private final String stateContainerName = "stateContainer";

	private CosmosDatabase database;
	private static CosmosContainer orderContainer;
	private static CosmosContainer errorContainer;
	private static CosmosContainer stateContainer;

	protected static Logger logger = LoggerFactory.getLogger(CosmosDbFunction.class.getSimpleName());

	@FunctionName("cosmosdbprocessor")
	public void eventHubProcessor(
			@EventHubTrigger(name = "msg", eventHubName = "event", dataType = "String", connection = "eventhubConnString", cardinality = Cardinality.ONE) String eventHubMessage,
			final ExecutionContext context) throws Exception {
		String jsonString = eventHubMessage;
		JSONObject jsonObject = new JSONObject(jsonString);
		jsonObject.put("id", System.currentTimeMillis() + "");
		jsonObject.put("id", System.currentTimeMillis() + "");
		String eventType = null;
		client = new CosmosClientBuilder().endpoint("https://cosmos-coffeecloud.documents.azure.com:443/")
				.key("fO1B53RO5bpffiabWDMo53nQaQRfm6uf5YhL74T1s9xOxVYfakFyqnuLf6rSolxoiHusljjkIXIADXJmziStyQ==")
				.preferredRegions(Collections.singletonList("West US")).consistencyLevel(ConsistencyLevel.EVENTUAL)
				.buildClient();
		CosmosDatabaseResponse cosmosDatabaseResponse = client.createDatabaseIfNotExists(databaseName);
		database = client.getDatabase(cosmosDatabaseResponse.getProperties().getId());

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
			Order order = (Order) getTheEvent(eventType, jsonObject);
			createContainerIfNotExists(orderContainerName, database);
			saveOrder(order);
			break;
		case "error":
			Error error = (Error) getTheEvent(eventType, jsonObject);
			createContainerIfNotExists(errorContainerName, database);
			saveError(error);
			break;
		case "state":
			State state = (State) getTheEvent(eventType, jsonObject);
			createContainerIfNotExists(stateContainerName, database);
			saveState(state);
			break;
		case "unknown":
			break;
		}
	}

	public static void createContainerIfNotExists(String containerName, CosmosDatabase database) throws Exception {
		CosmosContainerProperties containerProperties = null;
		CosmosContainerResponse cosmosContainerResponse = null;

		if (containerName == "orderContainer") {
			containerProperties = new CosmosContainerProperties(containerName, "/product");
			cosmosContainerResponse = database.createContainerIfNotExists(containerProperties,
					ThroughputProperties.createManualThroughput(400));
			orderContainer = database.getContainer(cosmosContainerResponse.getProperties().getId());
		} else if (containerName == "errorContainer") {
			containerProperties = new CosmosContainerProperties(containerName, "/error");
			cosmosContainerResponse = database.createContainerIfNotExists(containerProperties,
					ThroughputProperties.createManualThroughput(400));
			errorContainer = database.getContainer(cosmosContainerResponse.getProperties().getId());
		} else if (containerName == "stateContainer") {
			containerProperties = new CosmosContainerProperties(containerName, "/m");
			cosmosContainerResponse = database.createContainerIfNotExists(containerProperties,
					ThroughputProperties.createManualThroughput(400));
			stateContainer = database.getContainer(cosmosContainerResponse.getProperties().getId());
		}
	}

	public static void saveOrder(Order order) throws Exception {
		CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
		orderContainer.createItem(order, new PartitionKey(order.getProduct()),
				cosmosItemRequestOptions);
	}

	public static void saveError(Error error) throws Exception {
		CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
		errorContainer.createItem(error, new PartitionKey(error.getError()),
				cosmosItemRequestOptions);
	}

	public static void saveState(State state) throws Exception {
		CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
		stateContainer.createItem(state, new PartitionKey(state.getM()),
				cosmosItemRequestOptions);
	}

	public static Object getTheEvent(String eventType, JSONObject jsonObject) {
		if (eventType == "order") {
			String sn = jsonObject.getJSONObject("Origin").getString("SN");
			int fw = (int) jsonObject.getJSONObject("Origin").getNumber("FW");
			String product = jsonObject.getString("product");
			String id = jsonObject.getString("id");
			int gid = (int) jsonObject.getNumber("gid");
			long milliseconds = jsonObject.getJSONObject("timestamp").getLong("milliseconds");
			Origin origin = new Origin();
			Date date = new Date(milliseconds);
			origin.setSn(sn);
			origin.setFw(fw);
			Order order = new Order();
			order.setOrigin(origin);
			order.setGid(gid);
			order.setProduct(product);
			order.setId(id);
			order.setDate(date);
			return order;
		}
		if (eventType == "error") {
			String sn = jsonObject.getJSONObject("Origin").getString("SN");
			int fw = (int) jsonObject.getJSONObject("Origin").getNumber("FW");
			String e = jsonObject.getString("error");
			int code = (int) jsonObject.getNumber("code");
			String id = jsonObject.getString("id");
			long milliseconds = jsonObject.getJSONObject("timestamp").getLong("milliseconds");
			Origin origin = new Origin();
			Date date = new Date(milliseconds);
			origin.setSn(sn);
			origin.setFw(fw);
			Error error = new Error();
			error.setOrigin(origin);
			error.setError(e);
			error.setCode(code);
			error.setId(id);
			error.setDate(date);
			return error;
		}
		if (eventType == "state") {
			String sn = jsonObject.getJSONObject("Origin").getString("SN");
			int fw = (int) jsonObject.getJSONObject("Origin").getNumber("FW");
			String m = jsonObject.getString("m");
			Origin origin = new Origin();
			String id = jsonObject.getString("id");
			long milliseconds = jsonObject.getJSONObject("timestamp").getLong("milliseconds");
			Date date = new Date(milliseconds);
			origin.setSn(sn);
			origin.setFw(fw);
			State state = new State();
			state.setOrigin(origin);
			state.setId(id);
			state.setM(m);
			state.setDate(date);
			return state;
		}
		return null;
	}

	static class Order {
		String id;
		Origin origin;
		String product;
		int gid;
		Date date;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Origin getOrigin() {
			return origin;
		}

		public void setOrigin(Origin origin) {
			this.origin = origin;
		}

		public String getProduct() {
			return product;
		}
		
		public Date getDate() {
			return date;
		}

		public void setProduct(String product) {
			this.product = product;
		}

		public int getGid() {
			return gid;
		}

		public void setGid(int gid) {
			this.gid = gid;
		}
		
		public void setDate(Date date) {
			this.date = date;
		}
	}

	static class Error {
		String id;
		Origin origin;
		int code;
		String error;
		Date date;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Origin getOrigin() {
			return origin;
		}
		
		public void setDate(Date date) {
			this.date = date;
		}
		
		public Date getDate() {
			return date;
		}

		public void setOrigin(Origin origin) {
			this.origin = origin;
		}

		public int getCode() {
			return code;
		}

		public void setCode(int code) {
			this.code = code;
		}

		public String getError() {
			return error;
		}

		public void setError(String error) {
			this.error = error;
		}

	}

	static class State {
		String id;
		Origin origin;
		String m;
		Date date;
		
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Origin getOrigin() {
			return origin;
		}

		public void setOrigin(Origin origin) {
			this.origin = origin;
		}
		
		public void setDate(Date date) {
			this.date = date;
		}
		
		public Date getDate() {
			return date;
		}

		public String getM() {
			return m;
		}

		public void setM(String m) {
			this.m = m;
		}

	}

	static class Origin {
		String sn;
		int fw;

		public String getSn() {
			return sn;
		}

		public void setSn(String sn) {
			this.sn = sn;
		}

		public int getFw() {
			return fw;
		}

		public void setFw(int fw) {
			this.fw = fw;
		}
	}

}
