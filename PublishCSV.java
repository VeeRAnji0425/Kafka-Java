import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Properties;
import java.io.FileInputStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class PublishCSV 
{
       private static Producer<Integer, String> producer;
       private static final String topic= "cassie";
      public void initialize() 
       {
             Properties producerProps = new Properties();
             producerProps.put("metadata.broker.list", "138.197.12.79:9092");
             producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
             producerProps.put("request.required.acks", "1");
             ProducerConfig producerConfig = new ProducerConfig(producerProps);
             producer = new Producer<Integer, String>(producerConfig);
       }

    // Open the file
       public void publishMessage(Object[] args) throws Exception
       {
    	   String fl = args[0].toString();
   		   File file = new File(fl);
    	// File file = new File("D:/KafkaTestLogs/Test.log");
        FileInputStream fstream = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String msg = null;

       //Read File Line By Line
        while ((msg = br.readLine()) != null)   
        {
          // Print the content on the console
        		//System.out.println(msg);
           KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic, msg);
           producer.send(keyedMsg); // This publishes message on given topic
          //System.out.println("[" + msg + "]");
        }
       //Close the input stream
        br.close();
        
       }
       public static void main(String[] args) throws Exception 
       {
             PublishCSV kafkaProducer = new PublishCSV();
             // Initialize producer
             kafkaProducer.initialize();            
             // Publish message
             kafkaProducer.publishMessage(args);
             //Close the producer
            producer.close();
       }
   }
