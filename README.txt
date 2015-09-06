A few JMH tests for Solr code.

Building
========
mvn clean package

Generate Data
=============
Usage: java -cp ./target/benchmarks.jar DataGen </path/to/write/json/file> [size_of_document_mb=10MB]

Example:
java -cp ./target/benchmarks.jar DataGen ./a.json 15

Running
=======
java -server -Xmx2048M -Xms2048M -jar target/benchmarks.jar -wi 3 -i 3 -gc true ".*JavaBinCodecBenchmark.*"

java -server -Xmx2048M -Xms2048M -Dtests.json.path=./input.140.json -jar target/benchmarks.jar -wi 3 -i 10 -gc true -f 2 -prof gc ".*GrowableByteArrayDataOutputBenchmark.*"
java -server -Xmx2048M -Xms2048M -Dtests.seed=388281981 -jar target/benchmarks.jar -wi 3 -i 10 -gc true -f 2 -prof gc ".*GrowableByteArrayDataOutputBenchmark.*"
java -server -Xmx2048M -Xms2048M -Dtests.string.minlen=5 -Dtests.string.maxlen=64 -Dtests.string.num=1000 -jar target/benchmarks.jar -wi 3 -i 10 -gc true -f 2 -prof gc ".*GrowableByteArrayDataOutputBenchmark.*"

Links
=====
http://openjdk.java.net/projects/code-tools/jmh/
http://psy-lob-saw.blogspot.in/p/jmh-related-posts.html
https://github.com/nitsanw/jmh-samples
