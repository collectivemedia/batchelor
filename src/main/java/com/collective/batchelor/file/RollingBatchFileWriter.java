package com.collective.batchelor.file;


import com.collective.batchelor.util.BatchHandler;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class RollingBatchFileWriter implements BatchHandler<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RollingBatchFileWriter.class);
    static final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateHour().withZoneUTC();

    private final String basePath;
    private BatchFileWriter batchFileWriter;
    String currentFile;

    public RollingBatchFileWriter(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public boolean handle(List<String> messages) {
        try {
            createWriter();
        } catch (IOException e) {
            LOGGER.error("write failed", e);
            return true;
        }
        return batchFileWriter.handle(messages);
    }

    private void createWriter() throws IOException {
        String newPath = createFileName();
        if (!newPath.equals(currentFile)) {
            if (batchFileWriter != null) {
                batchFileWriter.done();
            }
            batchFileWriter = new BatchFileWriter(newPath);
            currentFile = newPath;
        }
    }

    String createFileName() {
        if (basePath.contains(".")) {
            int endIndex = basePath.lastIndexOf(".");
            String fileName = basePath.substring(0, endIndex);
            String suffix = basePath.substring(endIndex + 1, basePath.length());
            return fileName + "-" + dateFormatter.print(new DateTime()) + "." + suffix;
        }
        return basePath + "-" + dateFormatter.print(new DateTime());
    }

    @Override
    public void done() {
        if (batchFileWriter != null)
            batchFileWriter.done();
    }
}
