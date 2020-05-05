package com.video.collector;

import com.google.gson.JsonArray;
import com.video.util.PropertyFileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class VideoStreamCollector {

    private static final Logger logger = Logger.getLogger(VideoStreamCollector.class);

    public static void main(String[] args) throws Exception {

        String iotId = "8061";
        String iotPass = "pass123";

        // set producer properties
        Properties prop = PropertyFileReader.readPropertyFile();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"));
        properties.put("acks", prop.getProperty("kafka.acks"));
        properties.put("retries",prop.getProperty("kafka.retries"));
        properties.put("batch.size", prop.getProperty("kafka.batch.size"));
        properties.put("linger.ms", prop.getProperty("kafka.linger.ms"));
        properties.put("max.request.size", prop.getProperty("kafka.max.request.size"));
        properties.put("compression.type", prop.getProperty("kafka.compression.type"));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ArrayList<String> cameraEntryArray = new ArrayList<String>();

        String targetURL = "";
        HttpURLConnection connection = null;

        try {
            URL url = new URL("http://3.7.152.162/face/iot/getdetail?iotId="+iotId+"&iotPassword="+iotPass);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            BufferedReader in = new BufferedReader(new InputStreamReader( con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
//            System.out.println(response.toString());
            JSONObject myResponse = new JSONObject(response.toString());
            JSONArray cameraArray = myResponse.getJSONArray("cameraArray");
            System.out.println("Number of cameras" + cameraArray);
            for (int cIndex=0; cIndex < cameraArray.length(); cIndex++) {
                JSONObject camera = cameraArray.getJSONObject(cIndex);
                System.out.println(camera);
                String cameraEntry = camera.getString("rtsp") + "," + camera.getString("camId") + "," + String.valueOf(camera.getInt("delay")) + "," + camera.getString("kafkaTopic") + "," + camera.getString("cameraType");
                cameraEntryArray.add(cameraEntry);
            }
            System.out.println(cameraEntryArray);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }

        // generate event
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        generateIoTEvent(producer,prop.getProperty("kafka.topic"), cameraEntryArray);
    }

    private static void generateIoTEvent(Producer<String, String> producer, String topic, ArrayList<String> cameraEntryArray) throws Exception {

//        Logic for using multiple cameras
        for(int pIndex=0;pIndex<cameraEntryArray.size();pIndex++) {

            String cameraId = cameraEntryArray.get(pIndex).split(",")[1];
            String rtspLink  = cameraEntryArray.get(pIndex).split(",")[0];
            String topicForCamera = cameraEntryArray.get(pIndex).split(",")[3];
            Random random = new Random();
            int randomInteger = random.nextInt(100);
            Integer partitionForCamera = randomInteger;
            String delay = cameraEntryArray.get(pIndex).split(",")[2];
            String cameraType = cameraEntryArray.get(pIndex).split(",")[4];

            Thread.sleep(1000);
            if(cameraType.equals("faceRecog")) {
                // Keeps VideoCapture Object Cont. Open to save time
                Thread t = new Thread(new VideoEventGenerator(cameraId, rtspLink, producer, topicForCamera, 0, Integer.parseInt(delay), cameraType));
                t.start();
            } else if(cameraType.equals("socialDistance")){
                // Closes VideoCapture Object after each frame capture to save memory
                Thread t = new Thread(new VideoSDGenerator(cameraId, rtspLink, producer, topicForCamera, partitionForCamera, Integer.parseInt(delay), cameraType));
                t.start();
            }
        }
    }
}
