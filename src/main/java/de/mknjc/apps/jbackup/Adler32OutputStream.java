package de.mknjc.apps.jbackup;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.zip.Adler32;

public class Adler32OutputStream extends FilterOutputStream {
	private final Adler32 c = new Adler32();

	public Adler32OutputStream(final OutputStream out) {
		super(out);
	}

	@Override
	public void write(final int b) throws IOException {
		this.out.write(b);
		this.c.update(b);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {
		this.out.write(b, off, len);
		this.c.update(b, off, len);
	}

	/**
	 * writes the current checksum into the stream
	 */
	 public void writeChecksum(final ByteOrder order) throws IOException {
		 final long checksum = this.c.getValue();

		 if(order == ByteOrder.LITTLE_ENDIAN) {
			 this.write((int)(checksum       & 0xff));
			 this.write((int)(checksum >>  8 & 0xff));
			 this.write((int)(checksum >> 16 & 0xff));
			 this.write((int)(checksum >> 24 & 0xff));
		 } else {
			 this.write((int)(checksum >> 24 & 0xff));
			 this.write((int)(checksum >> 16 & 0xff));
			 this.write((int)(checksum >>  8 & 0xff));
			 this.write((int)(checksum       & 0xff));
		 }
	 }
}
