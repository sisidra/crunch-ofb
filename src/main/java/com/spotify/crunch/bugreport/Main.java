package com.spotify.crunch.bugreport;

import com.google.common.collect.Lists;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;

public class Main {

  public static void main(String[] args) {
    MRPipeline pipeline = new MRPipeline(Main.class, "OutputFormatBugreport", new Configuration());
    PCollection<String> pCollection = pipeline.create(Lists.newArrayList("line1", "line2"), Avros.strings());
    pCollection.write(new MyTarget());
    pipeline.done();
  }
}
