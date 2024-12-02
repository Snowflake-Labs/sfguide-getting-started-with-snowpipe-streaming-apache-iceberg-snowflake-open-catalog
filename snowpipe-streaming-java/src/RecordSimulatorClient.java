import java.io.FileInputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import java.security.KeyFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.interfaces.RSAPrivateKey;

public class RecordSimulatorClient {
  private static final String PROFILE_PATH = "snowflake.properties";
  private static boolean DEBUG=false;
  private static int NUM_ROWS=100;  // default rows to insert into landing iceberg table
  private static String SPEED="MAX";
  private static HashMap<Integer,String[]> DATA = new HashMap<Integer,String[]>();

  public static void main(String[] args) throws Exception {
    if(args!=null && args.length>0) SPEED=args[0];
    //Load profile from properties file
    Properties props=loadProfile();

    // Create a streaming ingest client
    try (SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("CLIENT").setProperties(props).build()) {

      // Create an open channel request on table T_STREAMINGINGEST
      OpenChannelRequest request1 =
          OpenChannelRequest.builder(props.getProperty("CHANNEL_NAME")+"_"+SPEED)
              .setDBName(props.getProperty("DATABASE"))
              .setSchemaName(props.getProperty("SCHEMA"))
              .setTableName(props.getProperty("TABLE"))
              .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

      // corresponds to the row number
      long startTime = System.nanoTime();
      for (int id = 1; id <= NUM_ROWS; id++) {
        // A Map will hold rowset (all columns)
        Map<String, Object> row = new HashMap<>();

        // Insert JSON payload into row:columns
        TimeUnit.MILLISECONDS.sleep(100); 
        System.out.print(id+" ");

        String[] record = DATA.get(id);  
	      row.put("vehicle_id", record[0]);
	      row.put("event_created_at", record[1]);
	      row.put("latitude", record[2]);
	      row.put("longitude", record[3]);
	      row.put("speed", record[4]);
	      row.put("engine_status", record[5]);
	      row.put("fuel_consumption_current", record[6]);
	      row.put("fuel_consumption_average", record[7]);
	      row.put("fuel_consumption_unit", record[8]);
	      row.put("hard_accelerations", record[9]);
	      row.put("smooth_accelerations", record[10]);
	      row.put("hard_brakes", record[11]);
	      row.put("smooth_brakes", record[12]);
	      row.put("sharp_turns", record[13]);
	      row.put("gentle_turns", record[14]);
	      row.put("maintenance_required", record[15]);

        InsertValidationResponse response = channel1.insertRow(row, String.valueOf(id));
        if (response.hasErrors()) {
          // Simply throw exception at first error
          throw response.getInsertErrors().get(0).getException();
        }
      }
      channel1.close().get();
      System.out.println("Rows Sent:  "+NUM_ROWS);
      System.out.println("Time to Send:  "+String.format("%.03f", (System.nanoTime() - startTime)*1.0/1000000000)+ " seconds");
      // Polling Snowflake to confirm delivery (using fetch offset token registered in Snowflake)
      int retryCount = 0;
      int maxRetries = 100;
      String expectedOffsetTokenInSnowflake=String.valueOf(NUM_ROWS);
      //String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
      for (String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();offsetTokenFromSnowflake == null
          || !offsetTokenFromSnowflake.equals(expectedOffsetTokenInSnowflake);) {
        if(DEBUG) System.out.println("Offset:  "+offsetTokenFromSnowflake);
        Thread.sleep(NUM_ROWS/1000);
        offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
        retryCount++;
        if (retryCount >= maxRetries) {
          System.out.println(
              String.format(
                  "Failed to look for required OffsetToken in Snowflake:%s after MaxRetryCounts:%s (%S)",
                  expectedOffsetTokenInSnowflake, maxRetries, offsetTokenFromSnowflake));
          System.exit(1);
        }
      }
      System.out.println("SUCCESSFULLY inserted " + NUM_ROWS + " rows");
      System.out.println("Total Time, including Confirmation:  "+String.format("%.03f", (System.nanoTime() - startTime)*1.0/1000000000)+ " seconds");
    }
  }

  private static Properties loadProfile() throws Exception {
    Properties props = new Properties();
    try {
      File f = new File(PROFILE_PATH);
      if (!f.exists()) throw new Exception("Unable to find profile file:  " + PROFILE_PATH);
      FileInputStream resource = new FileInputStream(PROFILE_PATH);
      props.load(resource);
      String num_rows=props.getProperty("NUM_ROWS");
      if(num_rows!=null) NUM_ROWS=Integer.parseInt(num_rows);
      String debug = props.getProperty("DEBUG");
      if (debug != null) DEBUG = Boolean.parseBoolean(debug);
      if (DEBUG) {
        for (Object key : props.keySet())
          System.out.println("  * DEBUG: " + key + ": " + props.getProperty(key.toString()));
      }

      if (props.getProperty("PRIVATE_KEY_FILE") != null) {
        String keyfile = props.getProperty("PRIVATE_KEY_FILE");
        File key = new File(keyfile);
        if (!(key).exists()) throw new Exception("Unable to find key file:  " + keyfile);
        String pkey = readPrivateKey(key);
        props.setProperty("private_key", pkey);
      }
      props.setProperty("scheme","https");
      props.setProperty("port","443");

      String file=props.getProperty("DATA_FILE");
      if(file==null) throw new Exception ("Parameter 'DATA_FILE' is required");
      File dataFile=new File(file);
      if(!dataFile.exists()) throw new Exception ("Data File '"+file+"' is not found");
      loadData(dataFile);

    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(-1);
    }
    return props;
  }

  public static <T> T instantiate(final String className, final Class<T> type) throws Exception {
    return type.cast(Class.forName(className).getDeclaredConstructor().newInstance());
  }

  private static String readPrivateKey(File file) throws Exception {
    String key = new String(Files.readAllBytes(file.toPath()), Charset.defaultCharset());
    String privateKeyPEM = key
            .replace("-----BEGIN PRIVATE KEY-----", "")
            .replaceAll(System.lineSeparator(), "")
            .replace("-----END PRIVATE KEY-----", "");
    if(DEBUG) {  // check key file is valid
      byte[] encoded = Base64.getDecoder().decode(privateKeyPEM);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      RSAPrivateKey k = (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
      System.out.println("* DEBUG: Provided Private Key is Valid:  ");
    }
    return privateKeyPEM;
  }

  private static void loadData(File f) throws Exception { 
      BufferedReader r = new BufferedReader(new FileReader(f));
      String line;
      int i = 1;
      while ((line = r.readLine()) != null) {
          String[] record = line.split(",");
          DATA.put(i,record);
          i++;
      }
      r.close();
  }  
}

