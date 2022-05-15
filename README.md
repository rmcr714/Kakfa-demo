This project is a use case to understand the working of kafka

Use Case: The use case is that we get the data from a source i.e wikipedia datasource. and put in elastic search

The services and flow : 
1.We have a producer 'WikimediaChangesProducer.java' which takes data from the wikipedia source and puts it on the kafka topic 'wikimedia.recentchange'
2. we have a consumer 'OpenSearchConsumer.java' which takes the data from the 'wikimedia.recentchange' topic and puts it on the opensearch index
3. This creates an event driven architecture where the producer is producing data to a topic and a consumer is consuming from it

In real life we have a seperate service which acts as producer and a seperate service which acts as the consumer and kafka is a middlemae between these two which stores data
The data can be anything, the producer might be an ecommerce app or a banking app  thats producing data to a specific topic and a consumer might use this data to do
analytics and come up with projections like what the person can buy next based on current purchase etc


SERVICE1 ------produces------> KAFKA-------->consumes---->SERVICE2 ---do anything with the data-




HOW TO RUN:
1. Clone it and open it using intellij
2. Start a local kafka cluster , u can find steps like how to create a local cluster and topics online
3. Create 3 topics namely 'first_topic' , 'second_topic' , 'wikimedia.recentchange', with 3 partitions
4. Although the 'wikimedia.recentchange' is the main topic that would be used by the main packages i.e kafka-producer-wikimedia and kafa-consumer-opensearch,
Still the package kafka-basics contains some code that shows the basics of kafka and contains producers and consumers that write and read from 'first_topic' and 'second_topic'
They could be used as beginner to see how we can produce and consume data from kafa before moving to the main project

5. you would also need to start a local or online opensearch cluster.
for this project i logged in to bonsai.io and created an opensearch cluster named KAFKA_DEMO. Its easy to create just google it and then provide its url in the 
OpensearchConsumer.java RestHighLevelClient method.

6. Start the consumer 'OpenSearchConsumer.java' and then start the producer 'WikimediaChangesProducer.java'  in this order , 
you would see that consumer consumes the messages that the producer is producing

7. You can change the duration of time till the producer producer by changing this line 'TimeUnit.MILLISECONDS.sleep(3000);' in 'WikimediaChangesProducer.java' to make it run 
for how many minutes or seconds u want to.
