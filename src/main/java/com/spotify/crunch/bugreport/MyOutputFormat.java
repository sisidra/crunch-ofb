package com.spotify.crunch.bugreport;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyOutputFormat<K, V> extends OutputFormat<K, V> {

  private static final Logger log = LoggerFactory.getLogger(MyOutputFormat.class);

  public static final String MY_CONFIG_VALUE = "my.config.value";
  public static final String MY_CONFIG_KEY = "my.config.key";


  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    log.info("TaskAttemptID (getRecordWriter): " + context.getTaskAttemptID().toString());

    String myConfigValue = context.getConfiguration().get(MY_CONFIG_KEY);
    log.info(MY_CONFIG_KEY + " (getRecordWriter): " + myConfigValue);
    if (!MY_CONFIG_VALUE.equals(myConfigValue)) {
      log.error("Wrong " + MY_CONFIG_KEY + " value in getRecordWriter!");// Never reached - OK
    }

    return new DummyRecordWriter<K, V>();
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    String myConfigValue = context.getConfiguration().get(MY_CONFIG_KEY);
    log.info(MY_CONFIG_KEY + " (checkOutputSpecs): " + myConfigValue);// null - NOK
    if (!MY_CONFIG_VALUE.equals(myConfigValue)) {
      log.error("Wrong " + MY_CONFIG_KEY + " value in checkOutputSpecs!");// Reached - NOK
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    log.info("TaskAttemptID (getOutputCommitter): " + context.getTaskAttemptID().toString());

    String myConfigValue = context.getConfiguration().get(MY_CONFIG_KEY);
    log.info(MY_CONFIG_KEY + " (getOutputCommitter): " + myConfigValue);
    if (!MY_CONFIG_VALUE.equals(myConfigValue)) {
      log.error("Wrong " + MY_CONFIG_KEY + " value in getOutputCommitter!");// Never reached - OK
    }

    return new DummyOutputCommitter();
  }

  private static class DummyRecordWriter<K, V> extends RecordWriter<K, V> {

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      // dummy
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      // dummy
    }
  }

  private static class DummyOutputCommitter extends OutputCommitter {

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      // dummy
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
      // dummy
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
      // dummy
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      // dummy
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
      // dummy
    }
  }
}
