package com.kafka.demo.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {

        String connString = "replace this with ur opensearch connection url";


        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }


    private static KafkaConsumer<String,String> createKafkaConsumer(){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";


        //Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");


        //Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);


        return consumer;
    }


    private static String extractId(String value) {
/************* DATA STRUCTURE ****************
 *
 * The structure of json data is like this
 * {schema: "/wikimedia/..." , meta {uri :"...",id:'this is what we want its unique',...other data}
 *
 * u can also see the data if u want by printing it here
 * *****/
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }


    public static void main(String[] args) throws IOException {


        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //first create an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();


        //create our kafka client
        KafkaConsumer<String,String> consumer = createKafkaConsumer();




        //create an index on opensearch if it doesnt exist already , wikimedia is our index name
        try(openSearchClient;consumer) {

        boolean indexExists= openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),RequestOptions.DEFAULT);

        if(!indexExists){
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("The Wikimedia index has been created successfully");
        }else{
            log.info("The wikimedia index already exists");
        }

        //we subscribe to the topic. The same topic is used in producer in wikimediaChangesProducer file
        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));


       //consumer code to consume the records and sending them to open search
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(3000));
            int recordCount = records.count();
            log.info("received "+recordCount+" records");

            //doing bulk transfer for better efficiency
            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record:records){

                String id = extractId(record.value()); // extract uinique id from data so that we dont publish same data twice to open search

                   //send the record to opensearch
                IndexRequest indexRequest = new IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON)
                        .id(id);

//              IndexResponse indexResponse =  openSearchClient.index(indexRequest,RequestOptions.DEFAULT);
               bulkRequest.add(indexRequest);

//                log.info("Inserted 1 document in OpenSearch ");
            }

            if(bulkRequest.numberOfActions()>0) {
                BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
           log.info("Inserted "+bulkResponse.getItems().length +"record(s)");
            }
        }

        }

        //create our kafka client








    }


}
