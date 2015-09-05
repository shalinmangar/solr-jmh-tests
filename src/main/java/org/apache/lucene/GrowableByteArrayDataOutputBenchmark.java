package org.apache.lucene;

import org.apache.lucene.util.GrowableByteArrayDataOutput;
import org.apache.lucene.util.GrowableByteArrayDataOutputWriteStr;
import org.apache.solr.common.util.JsonRecordReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@State(Scope.Thread)
public class GrowableByteArrayDataOutputBenchmark {

    String data;

    @Setup
    public void setUp() throws Exception {
        List<String> fieldMappings = new ArrayList<>();
        fieldMappings.add("id:/id");
        fieldMappings.add("content:/content");
        JsonRecordReader jsonRecordReader = JsonRecordReader.getInst("/", fieldMappings);
        try (FileReader r = new FileReader("/Users/shalinmangar/work/oss/solr-jmh-tests/input14.json")) {
            List<Map<String, Object>> records = jsonRecordReader.getAllRecords(r);
            assert records.size() == 1;
            assert !records.get(0).isEmpty();
            assert records.get(0).containsKey("content");
            data = (String) records.get(0).get("content");
        }
    }

    @Benchmark
    public void testWriteStringDefault() throws IOException {
        // use the chunkSize used in Lucene50StoredFieldsFormat.Mode.BEST_SPEED
        GrowableByteArrayDataOutput dataOutput = new GrowableByteArrayDataOutput(1 << 14);
        dataOutput.writeString(data);
    }

    @Benchmark
    public void testWriteStringOverloaded() throws IOException  {
        GrowableByteArrayDataOutputWriteStr dataOutput = new GrowableByteArrayDataOutputWriteStr(1 << 14);
        dataOutput.writeString(data);
    }
}
