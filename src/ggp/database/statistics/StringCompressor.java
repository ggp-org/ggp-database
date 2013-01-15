package ggp.database.statistics;

import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.ggp.galaxy.shared.crypto.external.Base64Coder;

public class StringCompressor {
    public static String compress(String x) {
        byte[] theBuffer = new byte[x.length()];
        Deflater theDeflater = new Deflater(1); 
        theDeflater.setInput(x.getBytes()); 
        theDeflater.finish();
        int theCompressedLength = theDeflater.deflate(theBuffer);
        return new String(Base64Coder.encode(theBuffer, 0, theCompressedLength));
    }
    public static String decompress(String x) {
        try {
            byte[] theBuffer = new byte[1024];
            Inflater theInflater = new Inflater(); 
            theInflater.setInput(Base64Coder.decode(x));
            ByteArrayOutputStream theByteStream = new ByteArrayOutputStream(x.length());
            do {
                int theByteLength = theInflater.inflate(theBuffer);
                theByteStream.write(theBuffer, 0, theByteLength);
            } while (!theInflater.finished() && theInflater.getRemaining() > 0);
            theByteStream.close();
            return new String(theByteStream.toByteArray(), "utf-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}