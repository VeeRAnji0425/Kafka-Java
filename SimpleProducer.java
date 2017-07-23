import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Properties;
import java.io.FileInputStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer 
{
       private static Producer<Integer, String> producer;
       private static final String topic= "INFO";
       private static final String topic1= "WARN";
       private static final String topic2= "ERROR";
      public void initialize() 
       {
             Properties producerProps = new Properties();
             producerProps.put("metadata.broker.list", "138.197.12.79:9092");
             producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
             producerProps.put("request.required.acks", "1");
             ProducerConfig producerConfig = new ProducerConfig(producerProps);
             producer = new Producer<Integer, String>(producerConfig);
       }
/*       public void publishMesssage() throws Exception{            
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));               
         while (true){
             System.out.print("Enter message to send to kafka broker(Press 'Y' to close producer): ");
           String msg = null;
           msg = reader.readLine(); // Read message from console
           //Define topic name and message
           KeyedMessage<Integer, String> keyedMsg =
                        new KeyedMessage<Integer, String>(topic, msg);
           producer.send(keyedMsg); // This publishes message on given topic
           if("Y".equals(msg)){ break; }
           System.out.println("--> Message [" + msg + "] sent.Check message on Consumer's program console");
         }
         return;
       } */

    // Open the file
       public void publishMesssage(Object[] args) throws Exception
       {
    	   String fl = args[0].toString();
   		   File file = new File(fl);
    	// File file = new File("D:/KafkaTestLogs/Test.log");
        FileInputStream fstream = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String msg = null;

       //Read File Line By Line
        while ((msg = br.readLine()) != null)   {
          // Print the content on the console
          // System.out.println (msg);
        	if(msg.contains("info"))
        	{
            //   System.out.println(msg);
                KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic, msg);
                producer.send(keyedMsg); // This publishes message on given topic
        	}
        	else if (msg.contains("warn"))
        	{
       //         System.out.println(msg);
                KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic1, msg);
                producer.send(keyedMsg); // This publishes message on given topic
        	}
        	else if (msg.contains("error"))
        	{
         //       System.out.println(msg);
                KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic2, msg);
                producer.send(keyedMsg); // This publishes message on given topic
        	}
//          System.out.println("[" + msg + "]");
       }
       //Close the input stream
       br.close();
       }
       public static void main(String[] args) throws Exception 
       {
             SimpleProducer kafkaProducer = new SimpleProducer();
             // Initialize producer
             kafkaProducer.initialize();            
             // Publish message
             kafkaProducer.publishMesssage(args);
             //Close the producer
             producer.close();
       }
   }
