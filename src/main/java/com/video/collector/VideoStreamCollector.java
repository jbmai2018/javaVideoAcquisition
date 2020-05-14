package com.video.collector;

import com.video.util.PropertyFileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.swing.*;
import java.awt.*;
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

        JTextField iotIdField = new JTextField(30);
        JTextField companyIdField = new JTextField(30);

        JPanel myPanel = new JPanel();
        myPanel.setLayout(new GridLayout(0, 2, 2, 2));
        myPanel.add(new JLabel("Desktop ID:"));
        myPanel.add(iotIdField);
        myPanel.add(new JLabel("Company ID:"));
        myPanel.add(companyIdField);

        int result = JOptionPane.showConfirmDialog(null, myPanel,
                "JBM Group - AI", JOptionPane.OK_CANCEL_OPTION);

        if (result == JOptionPane.OK_OPTION) {

            String iotId = iotIdField.getText();
            String iotPass = "pass123";
            String companyId = companyIdField.getText();
            String domain = "http://3.7.152.162";

            ArrayList<JSONObject> cameraEntryArray = new ArrayList<JSONObject>();

            HttpURLConnection connection = null;

            try {
                URL url = new URL(domain + "/face/iot/getdetail?iotId="+iotId+"&iotPassword="+iotPass+"&companyId="+companyId);
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
                    cameraEntryArray.add(camera);
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
            generateIoTEvent(cameraEntryArray);

        }
    }

    private static void generateIoTEvent(ArrayList<JSONObject> cameraEntryArray) throws Exception {

//        Logic for using multiple cameras
        for(int pIndex=0;pIndex<cameraEntryArray.size();pIndex++) {

            String cameraId = cameraEntryArray.get(pIndex).getString("camId");
            String rtspLink  = cameraEntryArray.get(pIndex).getString("rtsp");
            String topicForCamera = cameraEntryArray.get(pIndex).getString("kafkaTopic");
            Random random = new Random();
            int randomInteger = random.nextInt(100);
            Integer partitionForCamera = randomInteger;
            String delay = String.valueOf(cameraEntryArray.get(pIndex).getInt("delay"));
            String cameraType = cameraEntryArray.get(pIndex).getString("cameraType");
            String companyId = cameraEntryArray.get(pIndex).getString("companyId");
            String bootstrapServer = cameraEntryArray.get(pIndex).getString("bootstrapServer");


            Thread.sleep(1000);
            if(cameraType.equals("faceRecog")) {

                // set producer properties
                Properties prop = PropertyFileReader.readPropertyFile();
                Properties properties = new Properties();
                properties.put("bootstrap.servers", bootstrapServer);
//                properties.put("bootstrap.servers", "192.168.1.8:9092");
                properties.put("acks", prop.getProperty("kafka.acks"));
                properties.put("retries",prop.getProperty("kafka.retries"));
                properties.put("batch.size", prop.getProperty("kafka.batch.size"));
                properties.put("linger.ms", prop.getProperty("kafka.linger.ms"));
                properties.put("max.request.size", prop.getProperty("kafka.max.request.size"));
                properties.put("compression.type", prop.getProperty("kafka.compression.type"));
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


                Producer<String, String> producer = new KafkaProducer<String, String>(properties);
                // Keeps VideoCapture Object Cont. Open to save time

//                rtspLink = "rtsp://admin:bplmmx%40369@118.185.34.52:554/cam/realmonitor?channel=4&subtype=0";
//                rtspLink = "0";
//                companyId = "MX";

                Thread t = new Thread(new VideoEventGenerator(cameraId, rtspLink, producer, topicForCamera, 0, Integer.parseInt(delay), cameraType, companyId));
                t.start();
            }
            else if(cameraType.equals("socialDistance")){

                // set producer properties
                Properties prop = PropertyFileReader.readPropertyFile();
                Properties properties = new Properties();
                properties.put("bootstrap.servers", bootstrapServer);
                properties.put("acks", prop.getProperty("kafka.acks"));
                properties.put("retries",prop.getProperty("kafka.retries"));
                properties.put("batch.size", prop.getProperty("kafka.batch.size"));
                properties.put("linger.ms", prop.getProperty("kafka.linger.ms"));
                properties.put("max.request.size", prop.getProperty("kafka.max.request.size"));
                properties.put("compression.type", prop.getProperty("kafka.compression.type"));
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                Producer<String, String> producer2 = new KafkaProducer<String, String>(properties);
                // Closes VideoCapture Object after each frame capture to save memory
                Thread t = new Thread(new VideoSDGenerator(cameraId, rtspLink, producer2, topicForCamera, partitionForCamera, Integer.parseInt(delay), cameraType, companyId));
                t.start();
            }
        }
    }
}
