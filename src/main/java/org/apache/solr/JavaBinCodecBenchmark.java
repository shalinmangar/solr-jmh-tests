package org.apache.solr;

import org.apache.solr.common.util.*;
import org.openjdk.jmh.annotations.*;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@State(Scope.Thread)
public class JavaBinCodecBenchmark {

  String data;
  File tlogFile;
  FastOutputStream fos;

  @Setup
  public void setUp() throws Exception  {
    List<String> fieldMappings = new ArrayList<>();
    fieldMappings.add("id:/id");
    fieldMappings.add("content:/content");
    JsonRecordReader jsonRecordReader = JsonRecordReader.getInst("/", fieldMappings);
    try (FileReader r = new FileReader("/home/shalin/work/oss/solr-jmh-tests/input140.json")) {
      List<Map<String, Object>> records = jsonRecordReader.getAllRecords(r);
      assert records.size() == 1;
      assert !records.get(0).isEmpty();
      assert records.get(0).containsKey("content");
      data = (String) records.get(0).get("content");
    }

    tlogFile = new File("/home/shalin/temp/debug/lucenesolr-569/jimtests/tlog.log");
    if (tlogFile.exists())  {
      tlogFile.delete();
    }
    RandomAccessFile raf = new RandomAccessFile(this.tlogFile, "rw");
    FileChannel channel = raf.getChannel();
    OutputStream os = Channels.newOutputStream(channel);
    fos = new FastOutputStream(os, new byte[65536], 0);
  }

  @TearDown
  public void tearDown() throws Exception {
    fos.flush();
    fos.close();
    tlogFile.delete();
  }

  @Benchmark
  public void testDefaultWriteStr() throws IOException {
    JavaBinCodec codec = new JavaBinCodec();
    codec.init(fos);
    codec.writeStr(data);
  }

  @Benchmark
  public void testDirectBufferWriteStr() throws IOException {
    DirectBufferJavaBinCodec codec = new DirectBufferJavaBinCodec();
    codec.init(fos);
    codec.writeStr(data);
  }

  @Benchmark
  public void testDirectBufferNoScratchWriteStr() throws IOException {
    DirectBufferNoScratchJavaBinCodec codec = new DirectBufferNoScratchJavaBinCodec();
    codec.init(fos);
    codec.writeStr(data);
  }

  @Benchmark
  public void testDoublePassWriteStr() throws IOException {
    DoublePassJavaBinCodec codec = new DoublePassJavaBinCodec();
    codec.init(fos);
    codec.writeStr(data);
  }

  @Benchmark
  public void testDoublePassWriteWithScratchStr() throws IOException {
    DoublePassWithScratchJavaBinCodec codec = new DoublePassWithScratchJavaBinCodec();
    codec.init(fos);
    codec.writeStr(data);
  }

  @Benchmark
  public void testDoublePassCountingOutputStream() throws IOException {
    DoublePassCountingOutputStreamJavaBinCodec codec = new DoublePassCountingOutputStreamJavaBinCodec();
    codec.init(fos);
    codec.writeStr(data);
  }
}
