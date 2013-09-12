package com.collective.batchelor.file;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BatchFileWriterTest {

    @Test
    public void canWriteToFile() throws IOException, InterruptedException {
        File file = new File("test.txt");
        try {
            BatchFileWriter fileWriter = new BatchFileWriter(file.getAbsolutePath());
            assertThat(fileWriter.handle(Arrays.asList("test 1-2", "test 3-4"))).isTrue();
            fileWriter.done();
            List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
            assertThat(lines).hasSize(2).contains("test 1-2").contains("test 3-4");
        } finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void doesAppendToFile() throws IOException {
        File file = new File("test.txt");
        try {
            BatchFileWriter fileWriter = new BatchFileWriter(file.getAbsolutePath());
            assertThat(fileWriter.handle(Arrays.asList("test 1-2", "test 3-4"))).isTrue();
            fileWriter.done();

            BatchFileWriter fileWriter1 = new BatchFileWriter(file.getAbsolutePath());
            assertThat(fileWriter1.handle(Arrays.asList("test 5-6", "test 7-8"))).isTrue();
            fileWriter1.done();

            List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
            assertThat(lines).hasSize(4).contains("test 1-2").contains("test 3-4").contains("test 5-6").contains("test 7-8");
        } finally {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }

    @Test
    public void logsExceptionAndReturnsTrueIfWritingFails() throws IOException, TimeoutException, InterruptedException {
        BufferedWriter writer = mock(BufferedWriter.class);
        Mockito.doThrow(IOException.class).when(writer).write(anyString());
        BatchFileWriter fileWriter = new BatchFileWriter(writer);
        assertThat(fileWriter.handle(Arrays.asList("test 1-2", "test 3-4"))).isTrue();
        fileWriter.done();
        verify(writer).close();
    }

}
