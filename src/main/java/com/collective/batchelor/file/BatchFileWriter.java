package com.collective.batchelor.file;

import com.collective.batchelor.util.BatchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class BatchFileWriter implements BatchHandler<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchFileWriter.class);
    private final BufferedWriter bufferedWriter;

    public BatchFileWriter(String fileName) throws IOException {
        this(new BufferedWriter(new FileWriter(new File(fileName), true)));
    }

    BatchFileWriter(BufferedWriter writer) {
        bufferedWriter = writer;
    }

    @Override
    public boolean handle(List<String> messages) {
        for (String message : messages) {
            try {
                bufferedWriter.write(message);
                bufferedWriter.write("\n");
            } catch (IOException e) {
                LOGGER.error("write failed", e);
            }
        }
        return true;
    }

    @Override
    public void done() {
        try {
            bufferedWriter.close();
        } catch (IOException e) {
            LOGGER.error("close failed", e);
        }
    }
}
