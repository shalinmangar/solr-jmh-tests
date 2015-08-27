package org.apache.solr.common.util;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DirectBufferJavaBinCodec extends JavaBinCodec {
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
      if (bytes == null || bytes.length < 65536) bytes = new byte[65536];
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(maxSize);
      int sz = UTF16toUTF8(s, 0, end, byteBuffer, bytes);
      writeTag(STR, sz);
      byteBuffer.flip(); // make it ready to be read
      while (byteBuffer.hasRemaining()) {
        int len = Math.min(byteBuffer.remaining(), bytes.length);
        byteBuffer.get(bytes, 0, len);
        daos.write(bytes, 0, len);
      }
      byteBuffer.clear();
    }
  }

  /** Writes UTF8 into the byte buffer. The caller should ensure that
   * there is enough space for the worst-case scenario. The given scratch byte array
   * is used to buffer intermediate data before it is written to the byte buffer.
   *
   * @return the number of bytes written
   */
  public static int UTF16toUTF8(CharSequence s, int offset, int len, ByteBuffer byteBuffer, byte[] scratch) {
    final int end = offset + len;

    int upto = 0, totalBytes = 0;
    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (upto > scratch.length - 4)  {
        // a code point may take upto 4 bytes and we don't have enough space, so reset
        totalBytes += upto;
        byteBuffer.put(scratch, 0, upto);
        upto = 0;
      }

      if (code < 0x80)
        scratch[upto++] = (byte) code;
      else if (code < 0x800) {
        scratch[upto++] = (byte) (0xC0 | (code >> 6));
        scratch[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        scratch[upto++] = (byte)(0xE0 | (code >> 12));
        scratch[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        scratch[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            scratch[upto++] = (byte)(0xF0 | (utf32 >> 18));
            scratch[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            scratch[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            scratch[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        scratch[upto++] = (byte) 0xEF;
        scratch[upto++] = (byte) 0xBF;
        scratch[upto++] = (byte) 0xBD;
      }
    }

    totalBytes += upto;
    byteBuffer.put(scratch, 0, upto);

    return totalBytes;
  }
}
