package com.spotify.crunch.bugreport;

import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTarget implements MapReduceTarget {

  private final FormatBundle<MyOutputFormat> outputBundle;

  public MyTarget() {
    outputBundle = FormatBundle.forOutput(MyOutputFormat.class);
  }

  @Override
  public Target outputConf(String key, String value) {
    outputBundle.set(key, value);
    return this;
  }

  @Override
  public boolean handleExisting(WriteMode writeMode, long lastModifiedAt, Configuration conf) {
    return false;
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> pType) {
    handler.configure(this, pType);// SOOO RANDOM!

    return true;
  }

  @Override
  public Converter getConverter(PType<?> pType) {
    return pType.getConverter();
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> pType) {
    return null;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> pType, Path outputPath, String name) {
    outputBundle.set(MyOutputFormat.MY_CONFIG_KEY, MyOutputFormat.MY_CONFIG_VALUE);

    FileOutputFormat.setOutputPath(job, outputPath); // can this be automated in Crunch?

    CrunchOutputs.addNamedOutput(job, name, outputBundle, Object.class, Object.class);
  }
}
