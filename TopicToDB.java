import java.util.*;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import java.sql.*;


    public class TopicToDB
    {
    	private ConsumerConnector consumerConnector = null;
    private final String topic = "INFO";
    String myDriver = "com.mysql.jdbc.Driver";
    String myUrl = "jdbc:mysql://165.227.100.253/employees";
    
      

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
             
			try {
				Class.forName(myDriver);
				Connection conn = DriverManager.getConnection(myUrl, "juser", "root123");
				Statement stmt = conn.createStatement();
			
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
								Integer col1=new Integer(content.split(",")[0]);
								String col2=new String(content.split(",")[1]);
								String col3=new String(content.split(",")[2]);
								String col4=new String(content.split(",")[3]);
								String col5=new String(content.split(",")[4]);
								String col6=new String(content.split(",")[5]);
								//System.out.println(col1+" | "+col2+" | "+col3+" | "+col4+" | "+col5);
								//System.out.println(col1+"-<>-"+col2+"-<>-"+col3+"-<>-"+col4+"-<>-"+col5);
								String data="insert into employees_replica(emp_no, birth_date, first_name, last_name, gender, hire_date) values ('"+col1+"','"+col2+"','"+col3+"','"+col4+"','"+col5+"','"+col6+"')";
								stmt.executeUpdate(data);
								System.out.println("----------------------------------");
								System.out.println("Following Record Updated in DB :-");
								System.out.println(content);
					}
             }
             //Shutdown the consumer connector
					if (consumerConnector != null) consumerConnector.shutdown();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}        
       }

       public static void main(String[] args) throws InterruptedException {
             TopicToDB kafkaConsumer = new TopicToDB();
             // Configure Kafka consumer
             kafkaConsumer.initialize();
             // Start consumption
             kafkaConsumer.consume();
       }
   }