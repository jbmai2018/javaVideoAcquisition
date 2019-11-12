package com.video.collector;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Timer;

import com.video.util.NativeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.opencv.core.*;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.opencv.videoio.Videoio;

import javax.imageio.ImageIO;
import javax.swing.*;

public class VideoEventGenerator implements Runnable {
    private static final Logger logger = Logger.getLogger(VideoEventGenerator.class);
    private String cameraId;
    private String url;
    private Producer<String, String> producer;
    private String topic;
    private Integer partition;

    public VideoEventGenerator(String cameraId, String url, Producer<String, String> producer, String topic, Integer partition) {
        this.cameraId = cameraId;
        this.url = url;
        this.producer = producer;
        this.topic = topic;
        this.partition = partition;
    }

    //load OpenCV native lib
    static {
//        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        try {
            System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        } catch (UnsatisfiedLinkError e) {
            try {
                NativeUtils.loadLibraryFromJar("/" + System.mapLibraryName(Core.NATIVE_LIBRARY_NAME));
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        }
    }

    //custom ArrayList adapter
    class FrameArrayList {
        Mat mat;
        Timestamp timestamp;
        FrameArrayList(Mat mat, Timestamp timestamp) {
            this.mat = mat;
            this.timestamp = timestamp;
        }
    }

    @Override
    public void run() {
        logger.info("Processing cameraId "+cameraId+" with url "+url);
        try {
            generateEvent(cameraId,url,producer,topic,partition);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    //generate JSON events for frame
    private void generateEvent(String cameraId, String url, Producer<String, String> producer, String topic, Integer partition) throws Exception{
        VideoCapture camera = null;
        if(StringUtils.isNumeric(url)){
            camera = new VideoCapture(Integer.parseInt(url));
        } else {
            camera = new VideoCapture(url);
        }

        //works only with video files
        double fps = camera.get(Videoio.CAP_PROP_FPS);
        System.out.println( "(before setting) FPS: " + fps);
        camera.set(Videoio.CAP_PROP_FPS, 1.0);
        fps = camera.get(Videoio.CAP_PROP_FPS);
        System.out.println( "FPS: " + fps);

        //check camera working
        if (!camera.isOpened()) {
            Thread.sleep(5000);
            if (!camera.isOpened()) {
                logger.info("Error opening cameraId "+cameraId+" with url="+url+".Set correct file path or url in camera.url key of property file.");
                generateEvent(cameraId,url,producer,topic,partition);
//                throw new Exception("Error opening cameraId "+cameraId+" with url="+url+".Set correct file path or url in camera.url key of property file.");
            }
        }

        // Reading the next video frame from the camera
        Mat mat = new Mat();
        Gson gson = new Gson();

        int sizeOfFrameArray = 0;

        while (true) {
            Calendar cal = Calendar.getInstance();
            System.out.println("Loop Started !!");

            ArrayList<FrameArrayList> frameArray = new ArrayList<FrameArrayList>();
            //.toByteArray(); for each element

            while (System.currentTimeMillis() < cal.getTimeInMillis() + 1000) {
//                System.out.println("" + System.currentTimeMillis() + "       start  " + cal.getTimeInMillis() + 1000);
                try {
                    if(camera.read(mat)) {
                        FrameArrayList frameArrayList = new FrameArrayList(mat, new Timestamp(System.currentTimeMillis()));
                        frameArray.add(frameArrayList);
                    } else {
                        logger.info(camera.isOpened());
                        logger.info("Camera " + cameraId + " No Frame Recieved");
                        break;
                    }

                } catch (Exception e) {
                    logger.error(e.getMessage());
                    try {
                        generateEvent(cameraId,url,producer,topic,partition);
                    } catch (Exception e2) {
                        logger.info("Exiting Camera");
                        camera.release();
                        mat.release();
                        e2.printStackTrace();
                    }
                }
            }

            sizeOfFrameArray = frameArray.size();
            if(sizeOfFrameArray > 0) {
                FrameArrayList frameInfo = frameArray.get(0);
                mat = frameInfo.mat;
                MatOfByte matOfByte = new MatOfByte();
                Imgcodecs.imencode(".jpg", mat, matOfByte);
                byte[] data = matOfByte.toArray();

                String timestamp = frameInfo.timestamp.toString();
                JsonObject obj = new JsonObject();
                obj.addProperty("cameraId",cameraId);
                obj.addProperty("timestamp", timestamp);
                obj.addProperty("data", Base64.getEncoder().encodeToString(data));
                String json = gson.toJson(obj);
                producer.send(new ProducerRecord<String, String>(topic,partition,cameraId,json),new EventGeneratorCallback(cameraId));
                logger.info("Generated events for cameraId="+cameraId+" timestamp="+timestamp + " partition=" + partition);

            } else {
                logger.info("Starting Camera" + cameraId);
                generateEvent(cameraId,url,producer,topic,partition);
            }
        }
    }

    private static BufferedImage matToBufferedImage(Mat frame) {
        int type = 0;
        if (frame.channels() == 1) {
            type = BufferedImage.TYPE_BYTE_GRAY;
        } else if (frame.channels() == 3) {
            type = BufferedImage.TYPE_3BYTE_BGR;
        }
        BufferedImage image = new BufferedImage(frame.width(), frame.height(), type);
        WritableRaster raster = image.getRaster();
        DataBufferByte dataBuffer = (DataBufferByte) raster.getDataBuffer();
        byte[] data = dataBuffer.getData();
        frame.get(0, 0, data);

        return image;
    }

    private class EventGeneratorCallback implements Callback {
        private String camId;

        public EventGeneratorCallback(String camId) {
            super();
            this.camId = camId;
        }

        @Override
        public void onCompletion(RecordMetadata rm, Exception e) {
            if (rm != null) {
                logger.info("topic" + topic + " cameraId="+ camId + " partition=" + rm.partition());
            }
            if (e != null) {
                e.printStackTrace();
            }
        }
    }

}
