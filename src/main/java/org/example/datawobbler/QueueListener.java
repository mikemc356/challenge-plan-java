package org.example.datawobbler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

@Service
public class QueueListener {
    // private final RabbitTemplate rabbitTemplate;

    private static Logger logger = LoggerFactory.getLogger(QueueListener.class);

    @RabbitListener(queues = { "unpacker" })
    public void onFileReceived(String inputFile) throws InvalidKeyException, ErrorResponseException,
            InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException,
            ServerException, XmlParserException, IllegalArgumentException, IOException {
        logger.info("Unpacker event received: {}", inputFile);
        MinioClient minioClient = MinioClient.builder().endpoint("http://minio-mike-mcnamee.flows-dev-cluster-7c309b11fc78649918d0c8b91bcb5925-0000.eu-gb.containers.appdomain.cloud:80")
                .credentials("minio", "miniopassword").build();
        String targetFile = "/tmp/" + inputFile + ".formatted";
        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket("unpacked")
                        .object(inputFile)
                        .build());
                OutputStream outStream = new FileOutputStream(targetFile);) {

            // Read data from stream

            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outStream));
            while (true) {
                String line = reader.readLine();
                if ( line == null ) {
                    break;
                }
                System.out.println("Read line " + line);
                line = line.replace(">",">\n");
                writer.write(line);
            }
        } catch (Exception ex) {
            logger.error("Exception {}", ex);
            return;
        }

        minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket("formatted")
                        .object(inputFile)
                        .filename(targetFile)
                        .build());
    }

}
