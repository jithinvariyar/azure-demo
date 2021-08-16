package dk.scanomat.coffeecloud.functions;

import java.util.Collections;

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
import com.azure.cosmos.models.CosmosItemResponse;
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

		String eventType = null;
		try {
			client = new CosmosClientBuilder().endpoint("https://cosmos-coffeecloud.documents.azure.com:443/")
					.key("fO1B53RO5bpffiabWDMo53nQaQRfm6uf5YhL74T1s9xOxVYfakFyqnuLf6rSolxoiHusljjkIXIADXJmziStyQ==")
					.preferredRegions(Collections.singletonList("West US")).consistencyLevel(ConsistencyLevel.EVENTUAL)
					.buildClient();
		} catch (Exception e) {
			logger.info("I dont know");
		}

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
			createContainerIfNotExists(orderContainerName, database);
			saveOrder(jsonObject);
			break;
		case "error":
			createContainerIfNotExists(errorContainerName, database);
			saveError(jsonObject);
			break;
		case "state":
			createContainerIfNotExists(stateContainerName, database);
			saveState(jsonObject);
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

	public static void saveOrder(JSONObject jsonObject) throws Exception {
		CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
		CosmosItemResponse<JSONObject> item = orderContainer.createItem(jsonObject,
				new PartitionKey(jsonObject.get("product")), cosmosItemRequestOptions);
	}

	public static void saveError(JSONObject jsonObject) throws Exception {
		CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
		CosmosItemResponse<JSONObject> item = errorContainer.createItem(jsonObject,
				new PartitionKey(jsonObject.get("error")), cosmosItemRequestOptions);
	}

	public static void saveState(JSONObject jsonObject) throws Exception {
		CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
		CosmosItemResponse<JSONObject> item = stateContainer.createItem(jsonObject,
				new PartitionKey(jsonObject.get("m")), cosmosItemRequestOptions);
	}
}
