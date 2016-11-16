package com.anthonykim.datamgr.distributor;

import com.anthonykim.datamgr.hadoop.HdfsFileUploader;
import com.anthonykim.datamgr.model.KafkaDataFormat;

import lombok.Data;
import lombok.extern.log4j.Log4j;

import java.io.*;
import java.lang.ref.WeakReference;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by hyuk0 on 2016-11-04.
 */
@Log4j
@Data
public class MessageDistributorImpl implements MessageDistributor {
    private final String topicName;
    private final Map<Object, List<KafkaDataFormat>> kafkaDataMap;

    public MessageDistributorImpl(String topicName) {
        this.topicName = topicName;
        this.kafkaDataMap = new HashMap<>();
    }

    @Override
    public boolean sendMessageToHadoop(final String message) {
        final String fileName = getLocalFileNameFullyQualifiedPath(topicName);
        if (fileName == null)
            return false;

        File localFile = new File(fileName);

        if (!localFile.exists()) {
            final String statusFileName = ".data-manager/" + topicName + "-status";
            final String workingFileName = readLastWorkingFile(statusFileName);

            if (workingFileName != null)
                writeMessageToHadoopFileSystem(workingFileName);

            writeMessageToLocalFileSystem(message, localFile, false);
            writeLastWorkingFile(statusFileName, fileName);
        } else {
            writeMessageToLocalFileSystem(message, localFile, true);
        }
        return true;
    }

    private String getLocalFileNameFullyQualifiedPath(final String baseDir) {
        File baseDirectory = new File(baseDir);
        if (!baseDirectory.exists())
            if (!baseDirectory.mkdirs()) {
                log.error("Fail to create directory " + baseDir);
                return null;
            }

        final SimpleDateFormat dirDateFormat = new SimpleDateFormat("yyyyMMdd");
        final SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");

        Date nowDate = new Date();

        final String directoryPath = baseDir + "/" + dirDateFormat.format(nowDate);

        File dateDirectory = new File(directoryPath);
        if (!dateDirectory.exists())
            if (!dateDirectory.mkdirs()) {
                log.error("Fail to create directory " + directoryPath);
                return null;
            }

        return directoryPath + "/" + baseDir + "_" + fileDateFormat.format(nowDate);
    }

    private String readLastWorkingFile(final String statusFileName) {
        File baseDirectory = new File(".data-manager");
        if (!baseDirectory.exists())
            if (!baseDirectory.mkdirs()) {
                log.error("Fail to create directory \".data-manager\"");
                return null;
            }

        File statusFile = new File(statusFileName);
        if (!statusFile.exists())
            return null;

        String lastWorkingFileName = null;

        try (FileReader fileReader = new FileReader(statusFile);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            lastWorkingFileName = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lastWorkingFileName;
    }

    private void writeMessageToHadoopFileSystem(final String localFileName) {
        WeakReference<Thread> hdfsFileUploaderThread = new WeakReference<>(new Thread(new HdfsFileUploader(localFileName, localFileName)));
        hdfsFileUploaderThread.get().start();
    }

    private void writeMessageToLocalFileSystem(final String message, final File localFile,
                                               final boolean isAppendMode) {
        try (FileWriter writer = new FileWriter(localFile, isAppendMode)) {
            StringBuilder messageBuilder = new StringBuilder();

            if (isAppendMode)
                messageBuilder.append(",");
            messageBuilder.append(message);

            writer.write(messageBuilder.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeLastWorkingFile(final String statusFileName, final String newFileName) {
        File file = new File(statusFileName);

        try (FileWriter writer = new FileWriter(file, false)) {
            writer.write(newFileName);
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean sendMessageToDatabase(String message) {
        /* if you need to send message to database, just implement codes here. */
        return true;
    }
}