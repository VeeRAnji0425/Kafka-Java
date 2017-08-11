//import com.mysql.*;
import java.sql.*;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

 
public class DBToTopic
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
       public void MySQLCon() throws Exception
       {
    	   
    		  String msg=null;
    	      // create our mysql database connection
    	      String myDriver = "com.mysql.jdbc.Driver";
    	      String myUrl = "jdbc:mysql://165.227.100.253/employees";
    	      Class.forName(myDriver);
    	      Connection conn = DriverManager.getConnection(myUrl, "juser", "root123");
    	      
    	      // our SQL SELECT query. 
    	      // if you only need a few columns, specify them by name instead of using "*"
    	      String query = "SELECT * FROM employees";

    	      // create the java statement
    	      Statement st = conn.createStatement();
    	      
    	      // execute the query, and get a java resultset
    	      ResultSet rs = st.executeQuery(query);
    	      
    	      // iterate through the java resultset
    	      while (rs.next())
    	      {
    	        int id = rs.getInt("emp_no");
    	        Date bDate = rs.getDate("birth_date");
    	        String firstName = rs.getString("first_name");
    	        String lastName = rs.getString("last_name");
    	        boolean gender = rs.getBoolean("gender");
    	        Date hDate = rs.getDate("hire_date");
    	        msg = id+","+bDate+","+firstName+","+lastName+","+gender+","+hDate;
    	        KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic, msg);
    	        producer.send(keyedMsg); // This publishes message on given topic
    	        // print the results
    	        //System.out.println(msg);
    	      }
    	      conn.close();
    	    
    	  }
    	   
    	                        
       
       
public static void main(String[] args) throws Exception
{
             DBToTopic kafkaProducer = new DBToTopic();
             // Initialize producer
             kafkaProducer.initialize();           
			// Publish message
             kafkaProducer.MySQLCon();
             //Close the producer
             producer.close();
       }
   }