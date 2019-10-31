package com.video.collector;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Base64;

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
        }else{
            camera = new VideoCapture(url);
        }

        //getting the FPS
//        camera.get(Videoio.CAP_PROP_FPS);
        //setting the FPS
//        camera.set(Videoio.CAP_PROP_FPS,1.0);

        //check camera working
        if (!camera.isOpened()) {
            Thread.sleep(5000);
            if (!camera.isOpened()) {
                throw new Exception("Error opening cameraId "+cameraId+" with url="+url+".Set correct file path or url in camera.url key of property file.");
            }
        }

        // Reading the next video frame from the camera
        Mat mat = new Mat();
        Gson gson = new Gson();

        MatOfByte buffer = new MatOfByte();;
        MatOfInt compressParams;
        compressParams = new MatOfInt(Imgcodecs.CV_IMWRITE_JPEG_QUALITY, 1);

        while (true) {
            try {
                if(camera.read(mat)){
                    //resize image before sending
//                    Imgproc.resize(mat, mat, new Size(640, 480), 0, 0, Imgproc.INTER_CUBIC);
//                    Imgcodecs.imencode(".jpg", mat, buffer, compressParams);
//                    byte[] data = new byte[(int) (buffer.total() * buffer.elemSize())];
//                    buffer.get(0, 0, data);

                    MatOfByte matOfByte = new MatOfByte();
                    Imgcodecs.imencode(".jpg", mat, matOfByte);
                    byte[] data = matOfByte.toArray();

                    //store buffered image
//                    BufferedImage bufferedImage = matToBufferedImage(mat);
//                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
//                    ImageIO.write(bufferedImage, "jpg", bos );
//                    byte [] data = bos.toByteArray();

                    String timestamp = new Timestamp(System.currentTimeMillis()).toString();
                    JsonObject obj = new JsonObject();
                    obj.addProperty("cameraId",cameraId);
                    obj.addProperty("timestamp", timestamp);
                    obj.addProperty("data", Base64.getEncoder().encodeToString(data));
                    String json = gson.toJson(obj);
                    producer.send(new ProducerRecord<String, String>(topic,partition,cameraId,json),new EventGeneratorCallback(cameraId));
                    logger.info("Generated events for cameraId="+cameraId+" timestamp="+timestamp + " partition=" + partition);

                    //test base64 conversion
//                    String bytesEncoded = Base64.getEncoder().encodeToString("sahil is a good boy".getBytes());
//                    logger.info("base64 string : " + bytesEncoded);

//                    //Instantiate JFrame
//                    JFrame frame = new JFrame();
//                    //Set Content to the JFrame
//                    frame.getContentPane().add(new JLabel(new ImageIcon(bufferedImage)));
//                    frame.pack();
//                    frame.setVisible(true);
                }

                //every 2 seconds
                Thread.sleep(2000);

            } catch (Exception e) {
                logger.error(e.getMessage());
                continue;
            }
        }
//        logger.info("Exiting Cameraaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
//        camera.release();
//        mat.release();
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
