package org.apache.lucene;

import org.apache.lucene.util.GrowableByteArrayDataOutput;
import org.apache.lucene.util.GrowableByteArrayDataOutputCustom;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.util.JsonRecordReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

@State(Scope.Thread)
public class GrowableByteArrayDataOutputBenchmark {

    List<String> data;
    int idx;

    // use the chunkSize used in Lucene50StoredFieldsFormat.Mode.BEST_SPEED
    GrowableByteArrayDataOutput doDefault = new GrowableByteArrayDataOutput(1 << 14);
    GrowableByteArrayDataOutputCustom doCustom = new GrowableByteArrayDataOutputCustom(1 << 14);

    int numBufferedDocs = 0;

    final int chunkSize = 1 << 14;
    final int maxDocsPerChunk = 128;

    @Setup
    public void setUp() throws Exception {
        String outputDataPath = System.getProperty("tests.datagen.path");
        String dataPath = System.getProperty("tests.json.path");
        String seedStr = System.getProperty("tests.seed");
        String minStr = System.getProperty("tests.string.minlen");
        String maxStr = System.getProperty("tests.string.maxlen");
        String numStringsStr = System.getProperty("tests.string.num");

        Random random = new Random(seedStr != null ? Long.parseLong(seedStr) : System.currentTimeMillis());
        int min = minStr != null ? Integer.parseInt(minStr) : 1 + random.nextInt(5);
        int max = maxStr != null ? Integer.parseInt(maxStr) : min + random.nextInt(64);
        int numStrings = numStringsStr != null ? Integer.parseInt(numStringsStr) : 10000;

        data = new ArrayList<>(numStrings + 1);

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

        for (int i = 0; i < numStrings; i++) {
            data.add(TestUtil.randomRealisticUnicodeString(random, min, max));
        }

        if (outputDataPath != null) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputDataPath))) {
                for (String s : data) {
                    writer.write(s);
                    writer.write('\n');
                }
            }
        }
    }

    @Benchmark
    public void testWriteStringDefault() throws IOException {
        doDefault.writeString(data.get(idx));
        idx = (idx + 1) % data.size();

        numBufferedDocs++;
        if (doDefault.length >= chunkSize || numBufferedDocs >= maxDocsPerChunk)    {
            numBufferedDocs = 0;
            doDefault.length = 0;
        }
    }

    @Benchmark
    public void testWriteString1() throws IOException {
        doCustom.writeString(data.get(idx));
        idx = (idx + 1) % data.size();

        numBufferedDocs++;
        if (doCustom.length >= chunkSize || numBufferedDocs >= maxDocsPerChunk)    {
            numBufferedDocs = 0;
            doCustom.length = 0;
        }
    }

    @Benchmark
    public void testWriteString2() throws IOException {
        doCustom.writeString2(data.get(idx));
        idx = (idx + 1) % data.size();

        numBufferedDocs++;
        if (doCustom.length >= chunkSize || numBufferedDocs >= maxDocsPerChunk)    {
            numBufferedDocs = 0;
            doCustom.length = 0;
        }
    }

    @Benchmark
    public void testWriteString3() throws IOException   {
        doCustom.writeString2(data.get(idx));
        idx = (idx + 1) % data.size();

        numBufferedDocs++;
        if (doCustom.length >= chunkSize || numBufferedDocs >= maxDocsPerChunk)    {
            numBufferedDocs = 0;
            doCustom.length = 0;
        }
    }
}
