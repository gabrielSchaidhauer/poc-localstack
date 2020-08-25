import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;

public class CreateTableSample {

    public static void main(String... args) {
        final String table_name = "sample-table";

        final AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder
                .standard()
                .withCredentials(new CredentialsProvider())
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4569", Regions.US_EAST_1.name()))
                .build();

        final CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(10L,10L))
                .withTableName(table_name);

        try {
            if(dynamoClient.listTables(table_name).getTableNames().size() == 0) {
                CreateTableResult result = dynamoClient.createTable(request);
                System.out.println(result.getTableDescription().getTableName());
            } else {
                System.out.println("Table already exists");
            }

        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }

    }
}

class CredentialsProvider implements AWSCredentialsProvider {

    @Override
    public AWSCredentials getCredentials() {
        return new BasicAWSCredentials("anyAccessKey", "anySecretKey");
    }

    @Override
    public void refresh() {
        // Do Nothing
    }
}
