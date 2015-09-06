package org.apache.lucene;

import org.apache.lucene.util.GrowableByteArrayDataOutput;
import org.apache.lucene.util.GrowableByteArrayDataOutputWriteStr;
import org.apache.lucene.util.TestUtil;
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
import java.util.Random;

@State(Scope.Thread)
public class GrowableByteArrayDataOutputBenchmark {

    List<String> data;
    int idx;

    @Setup
    public void setUp() throws Exception {
        data = new ArrayList<>(10001);

        String dataPath = System.getProperty("tests.json.path");
        if (dataPath != null) {
            List<String> fieldMappings = new ArrayList<>();
            fieldMappings.add("id:/id");
            fieldMappings.add("content:/content");
            JsonRecordReader jsonRecordReader = JsonRecordReader.getInst("/", fieldMappings);
            try (FileReader r = new FileReader(dataPath)) {
                List<Map<String, Object>> records = jsonRecordReader.getAllRecords(r);
                assert records.size() == 1;
                assert !records.get(0).isEmpty();
                assert records.get(0).containsKey("content");
                data.add((String) records.get(0).get("content"));
            }
        }

        String seedStr = System.getProperty("tests.seed");
        Random random = new Random(seedStr != null ? Long.parseLong(seedStr) : System.currentTimeMillis());

        String minStr = System.getProperty("tests.string.minlen");
        String maxStr = System.getProperty("tests.string.maxlen");
        String numStringsStr = System.getProperty("tests.string.num");

        int min = minStr != null ? Integer.parseInt(minStr) : 1 + random.nextInt(5);
        int max = maxStr != null ? Integer.parseInt(maxStr) : min + random.nextInt(64);
        int numStrings = numStringsStr != null ? Integer.parseInt(numStringsStr) : 10000;
        for (int i = 0; i < numStrings; i++) {
            data.add(TestUtil.randomRealisticUnicodeString(random, min, max));
        }
    }

    @Benchmark
    public void testWriteStringDefault() throws IOException {
        // use the chunkSize used in Lucene50StoredFieldsFormat.Mode.BEST_SPEED
        GrowableByteArrayDataOutput dataOutput = new GrowableByteArrayDataOutput(1 << 14);
        dataOutput.writeString(data.get(idx));
        idx = (idx + 1) % data.size();
    }

    @Benchmark
    public void testWriteStringOverloaded() throws IOException {
        GrowableByteArrayDataOutputWriteStr dataOutput = new GrowableByteArrayDataOutputWriteStr(1 << 14);
        dataOutput.writeString(data.get(idx));
        idx = (idx + 1) % data.size();
    }
}
