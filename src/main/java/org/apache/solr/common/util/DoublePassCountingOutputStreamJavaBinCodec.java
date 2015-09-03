package org.apache.solr.common.util;

import java.io.IOException;
import java.io.OutputStream;

public class DoublePassCountingOutputStreamJavaBinCodec extends JavaBinCodec {
  @Override
  public void writeStr(String s) throws IOException {
    if (s == null) {
      writeTag(NULL);
      return;
    }
    int end = s.length();
    int maxSize = end * 3; // 3 is enough, see SOLR-7971

    if (maxSize <= 65536) {
      if (bytes == null || bytes.length < maxSize) bytes = new byte[maxSize];
      int sz = ByteUtils.UTF16toUTF8(s, 0, end, bytes, 0);
      writeTag(STR, sz);
      daos.write(bytes, 0, sz);
    } else {
      CountingNullOutputStream cos = new CountingNullOutputStream();
      writeUTF16toUTF8(s, 0, end, cos);
      writeTag(STR, cos.bytesWritten);
      writeUTF16toUTF8(s, 0, end, daos);
    }
  }

  public static class CountingNullOutputStream extends OutputStream {
    public int bytesWritten = 0;
    @Override
    public void write(int b) throws IOException {
      bytesWritten++;
    }
  }

  /** Writes UTF8 into the byte array, starting at offset.  The caller should ensure that
   * there is enough space for the worst-case scenario.
   * @return the number of bytes written
   */
  public static void writeUTF16toUTF8(CharSequence s, int offset, int len, OutputStream os) throws IOException {
    final int end = offset + len;

    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        os.write((byte) code);
      else if (code < 0x800) {
        os.write((byte) (0xC0 | (code >> 6)));
        os.write((byte) (0x80 | (code & 0x3F)));
      } else if (code < 0xD800 || code > 0xDFFF) {
        os.write((byte) (0xE0 | (code >> 12)));
        os.write((byte) (0x80 | ((code >> 6) & 0x3F)));
        os.write((byte) (0x80 | (code & 0x3F)));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            os.write((byte) (0xF0 | (utf32 >> 18)));
            os.write((byte) (0x80 | ((utf32 >> 12) & 0x3F)));
            os.write((byte) (0x80 | ((utf32 >> 6) & 0x3F)));
            os.write((byte) (0x80 | (utf32 & 0x3F)));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        os.write((byte) 0xEF);
        os.write((byte) 0xBF);
        os.write((byte) 0xBD);
      }
    }
  }
}
