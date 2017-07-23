//import java.io.*;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
 
public class kafkaProducer
{
       private static Producer<Integer, String> producer;
       private static final String topic= "INFO";
       public void initialize()
       {
             Properties producerProps = new Properties();
             producerProps.put("metadata.broker.list", "138.197.12.79:9092");
             producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
             producerProps.put("request.required.acks", "1");
             ProducerConfig producerConfig = new ProducerConfig(producerProps);
             producer = new Producer<Integer, String>(producerConfig);
       }  
       public void CassieCon() throws Exception
       {
    	   String serverIp = "104.131.63.177";
    	    String keyspace = "haritibcoblog";
    	    
    	    Cluster cluster = Cluster.builder()
    	            .addContactPoints(serverIp)
    	            .build();

    	    Session session = cluster.connect(keyspace);
    	    //BufferedReader rd=new BufferedReader(new InputStreamReader(System.in));
    	    
    	    //String cqlStatement = null;
    	    //cqlStatement = rd.readLine();
    	    String cqlStatement = "SELECT * FROM emp ";
    	    for (Row row : session.execute(cqlStatement)) 
    	    {
    	    		String msg = (row.toString());
    	        //System.out.println(msg);
    	        KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic, msg);
            producer.send(keyedMsg); // This publishes message on given topic
    	    }                     
       }
       
public static void main(String[] args) throws Exception
      {
             kafkaProducer kafkaProducer = new kafkaProducer();
             // Initialize producer
             kafkaProducer.initialize();           
			// Publish message
             kafkaProducer.CassieCon();
             //Close the producer
             producer.close();
       }
   }