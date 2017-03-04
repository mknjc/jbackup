package de.mknjc.apps.jbackup;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
	private final ByteBuffer bb;

	public ByteBufferInputStream(final ByteBuffer bb) {
		this.bb = bb;
	}

	@Override
	public int read() throws IOException {
		if(!this.bb.hasRemaining())
			return -1;

		return this.bb.get() & 0xff;
	}
	@Override
	public int read(final byte[] b, final int off, int len) throws IOException {
		if(!this.bb.hasRemaining())
			return -1;

		len = Math.min(len, this.bb.remaining());

		this.bb.get(b, off, len);

		return len;
	}

	@Override
	public int available() throws IOException {
		return this.bb.remaining();
	}
}
