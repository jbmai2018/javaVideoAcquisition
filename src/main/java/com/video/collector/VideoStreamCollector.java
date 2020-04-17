package com.video.collector;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.video.util.PropertyFileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class VideoStreamCollector {

    private static final Logger logger = Logger.getLogger(VideoStreamCollector.class);

    public static void main(String[] args) throws Exception {

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

        MongoClient mongoClient = MongoClients.create();
//        MongoClient mongoClient = MongoClients.create("mongodb://jbmTest3:jbm%40234@localhost:27017/?authMechanism=SCRAM-SHA-1&authSource=admin");
        MongoDatabase database = mongoClient.getDatabase("jbmDB");
        MongoCollection<Document> collection = database.getCollection("cameras");

        System.out.println("Number of cameras in DB : " + collection.countDocuments());

        ArrayList<String> cameraEntryArray = new ArrayList<String>();

        List<Document> cameras = (List<Document>) collection.find().into(
                new ArrayList<Document>());

        for (Document camera : cameras) {
            String camName = camera.getString("camName");
            //if(camName.equals("camera_192113224_CORP")) {
                List<Document> deploymentDetails = (List<Document>) camera.get("deploymentDetails");
                String x = (String) deploymentDetails.get(0).get("microserviceName");
                if(x.equals("faceRecog")) {
                    String rtspLink = "rtsp://";
                    Document login = (Document) camera.get("login");
                    String username = login.getString("username");
                    rtspLink += username;
                    String password = login.getString("password");
                    if(password.equals("password@123")){
                        password = "password%40123";
                    }
                    rtspLink += ":" + password;
                    rtspLink += "@";

                    Document hardware = (Document) camera.get("hardware");
                    String ip = hardware.getString("ip");
                    rtspLink += ip;
                    rtspLink += "/live/0/MAIN";

                    String camId = camera.getString("camName");
                    String cameraEntry = rtspLink + "," + camId;
                    cameraEntryArray.add(cameraEntry);
                }
            //}
//			System.out.println(cameraEntryArray);
        }

        // generate event
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        generateIoTEvent(producer,prop.getProperty("kafka.topic"), cameraEntryArray);
    }

    private static void generateIoTEvent(Producer<String, String> producer, String topic, ArrayList<String> cameraEntryArray) throws Exception {
//		String[] urls = videoUrl.split(",");
//		String[] ids = camId.split(",");
//		if(urls.length != ids.length){
//			throw new Exception("There should be same number of camera Id and url");
//		}

        //test array for testing on webcam
        ArrayList<String> cameraEntryArray2 = new ArrayList<String>();
        cameraEntryArray2.add("0,webcam");
//        cameraEntryArray2.add("rtsp://admin:password123@192.1.13.223/live/0/MAIN,webcam2");
//        cameraEntryArray2.add("rtsp://admin:password123@192.1.13.224/live/0/MAIN,webcam1");

//        cameraEntryArray2.add("rtsp://admin:Sahil12051994@@192.168.1.64:554/Streaming/Channels/101,webcam1");
//        cameraEntryArray2.add("rtsp://admin:Sahil12051994@@192.168.1.64:554/Streaming/Channels/201,webcam2");
//        cameraEntryArray2.add("rtsp://admin:Sahil12051994@@192.168.1.64:554/Streaming/Channels/301,webcam3");
//        cameraEntryArray2.add("rtsp://admin:Sahil12051994@@192.168.1.64:554/Streaming/Channels/401,webcam4");
//        cameraEntryArray2.add("rtsp://admin:password123@192.168.1.10:554/1,webcam1");
        cameraEntryArray = cameraEntryArray2;

        logger.info("Total urls to process "+cameraEntryArray.size());
        for(int pIndex=0;pIndex<cameraEntryArray.size();pIndex++) {
            Thread t = new Thread(new VideoEventGenerator(cameraEntryArray.get(pIndex).split(",")[1],cameraEntryArray.get(pIndex).split(",")[0],producer,topic, pIndex));
            t.start();
        }
    }
}
