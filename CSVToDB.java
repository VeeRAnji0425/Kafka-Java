import java.util.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import kafka.consumer.Consumer;
   import kafka.consumer.ConsumerConfig;
   import kafka.consumer.ConsumerIterator;
   import kafka.consumer.KafkaStream;
   import kafka.javaapi.consumer.ConsumerConnector;

    public class CSVToDB {
       private ConsumerConnector consumerConnector = null;
       private final String topic = "cassie";
       String serverIp = "104.131.63.177";
		String keyspace = "haritibcoblog";
		Cluster cluster = Cluster.builder().addContactPoint(serverIp).build();
		Session session = cluster.connect(keyspace);

       public void initialize() {
             Properties props = new Properties();
             props.put("log.retention.ms", "60000");
             props.put("zookeeper.connect", "138.197.12.79:2181");
             props.put("group.id", "BASIS");
            // props.put("zookeeper.session.timeout.ms", "1000");
             props.put("zookeeper.sync.time.ms", "300");
             props.put("auto.commit.interval.ms", "1000");
             ConsumerConfig conConfig = new ConsumerConfig(props);
             consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
       }

       public void consume() {
             //Key = topic name, Value = No. of threads for topic
    	   		 Map<String, Integer> topicCount = new HashMap<String, Integer>();       
             topicCount.put(topic, new Integer(1));
            
             //ConsumerConnector creates the message stream for each topic
             Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                   consumerConnector.createMessageStreams(topicCount);         
            
             // Get Kafka stream for topic 'mytopic'
             List<KafkaStream<byte[], byte[]>> kStreamList =consumerStreams.get(topic);
             // Iterate stream using ConsumerIterator
             for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) 
             {	
                    ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();  
					while (consumerIte.hasNext())
					{
								String content=new String(consumerIte.next().message());
								String col1=new String(content.split(",")[0]);
								String col2=new String(content.split(",")[1]);
								String col3=new String(content.split(",")[2]);
								String col4=new String(content.split(",")[3]);
								String col5=new String(content.split(",")[4]);
								//System.out.println(col1+" | "+col2+" | "+col3+" | "+col4+" | "+col5);
								//System.out.println(col1+"-<>-"+col2+"-<>-"+col3+"-<>-"+col4+"-<>-"+col5);
								String data="insert into emp1(emp_num, emp_city, emp_name, emp_phone, emp_sal) values ('"+col1+"','"+col2+"','"+col3+"','"+col4+"','"+col5+"')";
								session.execute(data); 
								//System.out.println("Updated - Please Query the DB now");
					}
             }
             //Shutdown the consumer connector
					if (consumerConnector != null) consumerConnector.shutdown();          
       }

       public static void main(String[] args) throws InterruptedException {
             CSVToDB kafkaConsumer = new CSVToDB();
             // Configure Kafka consumer
             kafkaConsumer.initialize();
             // Start consumption
             kafkaConsumer.consume();
       }
   }