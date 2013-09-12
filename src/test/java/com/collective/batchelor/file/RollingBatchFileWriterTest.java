package com.collective.batchelor.file;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.fest.assertions.api.Assertions.assertThat;

public class RollingBatchFileWriterTest {

    public static final SimpleDateFormat UTC_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    static {
        UTC_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @After
    public void resetDate() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void checkDateFormat() throws ParseException {
        DateTime dateTime = new DateTime(UTC_DATE_FORMAT.parse("2013-08-13 15:23:23:999"));
        assertThat(RollingBatchFileWriter.dateFormatter.print(dateTime)).isEqualTo("2013-08-13T15");
    }

    @Test
    public void doesWriteToFileWithGeneratedTimestamp() throws IOException, ParseException {
        DateTime dateTime = new DateTime(UTC_DATE_FORMAT.parse("2013-08-13 15:23:23:999"));
        DateTimeUtils.setCurrentMillisFixed(dateTime.getMillis());

        RollingBatchFileWriter fileWriter = new RollingBatchFileWriter("test");
        assertThat(fileWriter.handle(Arrays.asList("test 1-2", "test 3-4"))).isTrue();
        fileWriter.done();

        assertThat(fileWriter.currentFile).isEqualTo("test-2013-08-13T15");

        File file = new File(fileWriter.currentFile);
        List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
        assertThat(lines).hasSize(2).contains("test 1-2").contains("test 3-4");
        assertThat(file.delete()).isTrue();
    }

    @Test
    public void willUseDateBeforeFileSuffix() throws ParseException {
        DateTime dateTime = new DateTime(UTC_DATE_FORMAT.parse("2013-08-13 15:23:23:999"));
        DateTimeUtils.setCurrentMillisFixed(dateTime.getMillis());

        RollingBatchFileWriter fileWriter = new RollingBatchFileWriter("test.log");
        assertThat(fileWriter.createFileName()).isEqualTo("test-2013-08-13T15.log");

        fileWriter = new RollingBatchFileWriter("test");
        assertThat(fileWriter.createFileName()).isEqualTo("test-2013-08-13T15");

        fileWriter = new RollingBatchFileWriter("test.somemore.log");
        assertThat(fileWriter.createFileName()).isEqualTo("test.somemore-2013-08-13T15.log");

        fileWriter = new RollingBatchFileWriter("/home/bla/karli.lotti/test.log");
        assertThat(fileWriter.createFileName()).isEqualTo("/home/bla/karli.lotti/test-2013-08-13T15.log");
    }


    @Test
    public void willSwitchFileWhenHourChanges() throws ParseException, IOException {
        DateTime dateTime = new DateTime(UTC_DATE_FORMAT.parse("2013-08-13 15:23:23:999"));
        DateTimeUtils.setCurrentMillisFixed(dateTime.getMillis());

        RollingBatchFileWriter fileWriter = new RollingBatchFileWriter("test");
        assertThat(fileWriter.handle(Arrays.asList("test 1-2", "test 3-4"))).isTrue();

        String firstFile = "test-2013-08-13T15";
        assertThat(fileWriter.currentFile).isEqualTo(firstFile);

        dateTime = new DateTime(UTC_DATE_FORMAT.parse("2013-08-13 16:23:23:999"));
        DateTimeUtils.setCurrentMillisFixed(dateTime.getMillis());

        assertThat(fileWriter.handle(Arrays.asList("test 5-6", "test 7-8"))).isTrue();
        fileWriter.done();

        assertThat(fileWriter.currentFile).isEqualTo("test-2013-08-13T16");
        File file = new File(fileWriter.currentFile);
        List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
        assertThat(lines).hasSize(2).contains("test 5-6").contains("test 7-8");
        assertThat(file.delete()).isTrue();

        assertThat(new File(firstFile).delete()).isTrue();
    }

}
