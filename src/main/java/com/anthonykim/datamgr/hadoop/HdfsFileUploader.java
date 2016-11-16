package com.anthonykim.datamgr.hadoop;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import com.anthonykim.datamgr.common.PropertiesUtil;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by hyuk0 on 2016-11-01.
 */
@Log4j
@Data
@AllArgsConstructor
public class HdfsFileUploader implements Runnable {
    private String source;
    private String destination;

    @Override
    public void run() {
        log.info("HdfsFileUploader started...");
        Properties hadoopCoreProps = PropertiesUtil.getExternalProperties("conf/hadoop-core.properties");
        if (hadoopCoreProps == null)
            hadoopCoreProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-hadoop-core.properties");

        System.setProperty("hadoop.home.dir", "/");

        try {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopCoreProps.getProperty("hadoop.job.ugi"));
            ugi.doAs(new UserGroupInformationAction(hadoopCoreProps));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Data
    @AllArgsConstructor
    private class UserGroupInformationAction implements PrivilegedExceptionAction<Void> {
        private Properties hadoopCoreProps;

        @Override
        public Void run() throws Exception {
            Configuration config = new Configuration();
            config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            for (Object key : hadoopCoreProps.keySet())
                config.set((String) key, (String) hadoopCoreProps.get(key));

            try (FileSystem fs = FileSystem.get(URI.create(destination), config);
                 InputStream in = new BufferedInputStream(new FileInputStream(source));
                 OutputStream out = fs.create(new Path(destination))) {

                log.info("Connecting to -- " + config.get("fs.defaultFS"));

                IOUtils.copyBytes(in, out, 4096/*IO UNIT SIZE*/, true);

                log.info(destination + " copied to hadoop file system.");
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}