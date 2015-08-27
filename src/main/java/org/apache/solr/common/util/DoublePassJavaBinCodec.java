package org.apache.solr.common.util;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DoublePassJavaBinCodec extends JavaBinCodec {
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
      int sz = calcUTF16toUTF8Length(s, 0, end);
      writeTag(STR, sz);
      writeUTF16toUTF8(s, 0, end, daos);
    }
  }

  /** Writes UTF8 into the byte array, starting at offset.  The caller should ensure that
   * there is enough space for the worst-case scenario.
   * @return the number of bytes written
   */
  public static void writeUTF16toUTF8(CharSequence s, int offset, int len, FastOutputStream fos) throws IOException {
    final int end = offset + len;

    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        fos.write((byte) code);
      else if (code < 0x800) {
        fos.write((byte) (0xC0 | (code >> 6)));
        fos.write((byte)(0x80 | (code & 0x3F)));
      } else if (code < 0xD800 || code > 0xDFFF) {
        fos.write((byte)(0xE0 | (code >> 12)));
        fos.write((byte)(0x80 | ((code >> 6) & 0x3F)));
        fos.write((byte)(0x80 | (code & 0x3F)));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            fos.write((byte)(0xF0 | (utf32 >> 18)));
            fos.write((byte)(0x80 | ((utf32 >> 12) & 0x3F)));
            fos.write((byte)(0x80 | ((utf32 >> 6) & 0x3F)));
            fos.write((byte)(0x80 | (utf32 & 0x3F)));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        fos.write((byte) 0xEF);
        fos.write((byte) 0xBF);
        fos.write((byte) 0xBD);
      }
    }
  }

  /**
   * Writes UTF8 into the byte array, starting at offset.  The caller should ensure that
   * there is enough space for the worst-case scenario.
   *
   * @return the number of bytes written
   */
  public static int calcUTF16toUTF8Length(CharSequence s, int offset, int len) {
    final int end = offset + len;

    int res = 0;
    for (int i = offset; i < end; i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        res++;
      else if (code < 0x800) {
        res += 2;
      } else if (code < 0xD800 || code > 0xDFFF) {
        res += 3;
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end - 1)) {
          int utf32 = (int) s.charAt(i + 1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            i++;
            res += 4;
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        res += 3;
      }
    }

    return res;
  }
}
