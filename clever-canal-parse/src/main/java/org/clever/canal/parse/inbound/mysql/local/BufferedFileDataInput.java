package org.clever.canal.parse.inbound.mysql.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

@SuppressWarnings({"unused", "WeakerAccess"})
public class BufferedFileDataInput {

    private static final Logger logger = LoggerFactory.getLogger(BufferedFileDataInput.class);
    // Read parameters.
    private File file;
    private int size;

    // Variables to control reading.
    private FileInputStream fileInput;
    private BufferedInputStream bufferedInput;
    private DataInputStream dataInput;
    private long offset;
    private FileChannel fileChannel;

    public BufferedFileDataInput(File file, int size) {
        this.file = file;
        this.size = size;
    }

    public BufferedFileDataInput(File file) {
        this(file, 1024);
    }

    public long available() throws IOException {
        return fileChannel.size() - offset;
    }

    public long skip(long bytes) throws IOException {
        long bytesSkipped = bufferedInput.skip(bytes);
        offset += bytesSkipped;
        return bytesSkipped;
    }

    public void seek(long seekBytes) throws IOException, InterruptedException {
        fileInput = new FileInputStream(file);
        fileChannel = fileInput.getChannel();

        try {
            fileChannel.position(seekBytes);
        } catch (ClosedByInterruptException e) {
            throw new InterruptedException();
        }
        bufferedInput = new BufferedInputStream(fileInput, size);
        dataInput = new DataInputStream(bufferedInput);
        offset = seekBytes;
    }

    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    public void readFully(byte[] bytes, int start, int len) throws IOException {
        dataInput.readFully(bytes, start, len);
        offset += len;
    }

    public void close() {
        try {
            if (fileChannel != null) {
                fileChannel.close();
                fileInput.close();
            }
        } catch (IOException e) {
            logger.warn("Unable to close buffered file reader: file=" + file.getName() + " exception=" + e.getMessage());
        }
        fileChannel = null;
        fileInput = null;
        bufferedInput = null;
        dataInput = null;
        offset = -1;
    }
}
