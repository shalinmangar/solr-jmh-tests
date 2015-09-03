import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;

public class DataGen {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java -cp ./target/benchmarks.jar DataGen </path/to/write/json/file> [size_of_document_mb=10MB]");
      System.exit(1);
    }

    File path = new File(args[0]);
    if (!path.getParentFile().exists()) {
      System.err.println("Parent directory of path: " + args[0] + " does not exist.");
      System.exit(1);
    }
    if (!path.getParentFile().canWrite()) {
      System.err.println("Specified path: " + args[0] + " is not writable.");
      System.exit(1);
    }

    long targetSize = 10 * 1024L* 1024L; // 10 MB
    if (args.length > 1) {
      targetSize = Long.parseLong(args[1]) * 1024L * 1024L;
    }

    URL url = new URL("http://www.gutenberg.org/ebooks/1322.txt.utf-8");
    System.out.println("Making request to: " + url.toString());
    try (InputStream in = url.openStream()) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));

      CharArr out = new CharArr();
      JSONWriter jsonWriter = new JSONWriter(out);

      jsonWriter.startArray();
      jsonWriter.startObject();

      jsonWriter.writeString("content");
      jsonWriter.writeNameSeparator();


      char[] scratch = new char[1 << 15];
      int len = 0, count = 0;

      CharArr tmp = new CharArr(1 << 15);
      while ((len = reader.read(scratch)) != -1) {
        tmp.write(scratch, 0, len);
        count += len;
      }

      jsonWriter.writeStringStart();

      for (int i = 0; i < targetSize / count; i++) {
        jsonWriter.writeStringChars(tmp);
      }
      jsonWriter.writeStringEnd();

      jsonWriter.endObject();
      jsonWriter.endArray();

      try (FileWriter writer = new FileWriter(args[0])) {
        writer.append(out);
      }
    }
  }
}
