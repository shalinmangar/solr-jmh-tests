package org.apache.lucene.util;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;

public class GrowableByteArrayDataOutputCustom extends DataOutput {

    private static final long HALF_SHIFT = 10;
    private static final int SURROGATE_OFFSET =
            Character.MIN_SUPPLEMENTARY_CODE_POINT -
                    (UnicodeUtil.UNI_SUR_HIGH_START << HALF_SHIFT) - UnicodeUtil.UNI_SUR_LOW_START;

    /**
     * The bytes
     */
    public byte[] bytes;
    /**
     * The length
     */
    public int length;

    /**
     * Create a {@link GrowableByteArrayDataOutput} with the given initial capacity.
     */
    public GrowableByteArrayDataOutputCustom(int cp) {
        this.bytes = new byte[ArrayUtil.oversize(cp, 1)];
        this.length = 0;
    }

    /**
     * Encode characters from this String, starting at offset
     * for length characters. It is the responsibility of the
     * caller to make sure that the destination array is large enough.
     */
    public static int UTF16toUTF8(final CharSequence s, final int offset, final int length, byte[] out) {
        return UTF16toUTF8(s, offset, length, out, 0);
    }

    /**
     * Encode characters from this String, starting at offset
     * for length characters. Output to the destination array
     * will begin at {@code outOffset}. It is the responsibility of the
     * caller to make sure that the destination array is large enough.
     * <p>
     * note this method returns the final output offset (outOffset + number of bytes written)
     */
    public static int UTF16toUTF8(final CharSequence s, final int offset, final int length, byte[] out, int outOffset) {
        final int end = offset + length;

        int upto = outOffset;
        for (int i = offset; i < end; i++) {
            final int code = (int) s.charAt(i);

            if (code < 0x80)
                out[upto++] = (byte) code;
            else if (code < 0x800) {
                out[upto++] = (byte) (0xC0 | (code >> 6));
                out[upto++] = (byte) (0x80 | (code & 0x3F));
            } else if (code < 0xD800 || code > 0xDFFF) {
                out[upto++] = (byte) (0xE0 | (code >> 12));
                out[upto++] = (byte) (0x80 | ((code >> 6) & 0x3F));
                out[upto++] = (byte) (0x80 | (code & 0x3F));
            } else {
                // surrogate pair
                // confirm valid high surrogate
                if (code < 0xDC00 && (i < end - 1)) {
                    int utf32 = (int) s.charAt(i + 1);
                    // confirm valid low surrogate and write pair
                    if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
                        utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
                        i++;
                        out[upto++] = (byte) (0xF0 | (utf32 >> 18));
                        out[upto++] = (byte) (0x80 | ((utf32 >> 12) & 0x3F));
                        out[upto++] = (byte) (0x80 | ((utf32 >> 6) & 0x3F));
                        out[upto++] = (byte) (0x80 | (utf32 & 0x3F));
                        continue;
                    }
                }
                // replace unpaired surrogate or out-of-order low surrogate
                // with substitution character
                out[upto++] = (byte) 0xEF;
                out[upto++] = (byte) 0xBF;
                out[upto++] = (byte) 0xBD;
            }
        }
        //assert matches(s, offset, length, out, upto);
        return upto;
    }

    /**
     * Calculates the number of UTF8 bytes necessary to write a UTF16 string.
     *
     * @return the number of bytes written
     */
    public static int calcUTF16toUTF8Length(final CharSequence s, final int offset, final int len) {
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
                res += 3;
            }
        }

        return res;
    }

    @Override
    public void writeByte(byte b) {
        if (length >= bytes.length) {
            bytes = ArrayUtil.grow(bytes);
        }
        bytes[length++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int off, int len) {
        final int newLength = length + len;
        bytes = ArrayUtil.grow(bytes, newLength);
        System.arraycopy(b, off, bytes, length, len);
        length = newLength;
    }

    @Override
    public void writeString(String string) throws IOException {
        int numBytes = calcUTF16toUTF8Length(string, 0, string.length());
        writeVInt(numBytes);
        bytes = ArrayUtil.grow(bytes, length + numBytes);
        length = UTF16toUTF8(string, 0, string.length(), bytes, length);
    }

    public void writeString2(String string) throws IOException {
        int maxLen = string.length() * UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR;
        if (maxLen <= 65536)  {
            super.writeString(string);
        } else  {
            int numBytes = calcUTF16toUTF8Length(string, 0, string.length());
            writeVInt(numBytes);
            bytes = ArrayUtil.grow(bytes, length + numBytes);
            length = UTF16toUTF8(string, 0, string.length(), bytes, length);
        }
    }
}
