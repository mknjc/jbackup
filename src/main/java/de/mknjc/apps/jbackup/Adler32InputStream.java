package de.mknjc.apps.jbackup;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.util.zip.Adler32;

public class Adler32InputStream extends FilterInputStream {
	private final Adler32 cksum = new Adler32();

	public Adler32InputStream(final InputStream in) {
		super(in);
	}

	@Override
	public int read() throws IOException {
		final int b = this.in.read();
		if (b != -1) {
			this.cksum.update(b);
		}
		return b;
	}

	@Override
	public int read(final byte[] buf, final int off, int len) throws IOException {
		len = this.in.read(buf, off, len);
		if (len != -1) {
			this.cksum.update(buf, off, len);
		}
		return len;
	}

	@Override
	public long skip(final long n) throws IOException {
		final byte[] buf = new byte[512];
		long total = 0;
		while (total < n) {
			long len = n - total;
			len = this.read(buf, 0, len < buf.length ? (int)len : buf.length);
			if (len == -1) {
				return total;
			}
			total += len;
		}
		return total;
	}

	public boolean readChecksum(final ByteOrder order) throws IOException {
		final int expected = (int)(this.cksum.getValue() & 0xffffffffL);

		final byte[] readed = new byte[4];

		if(this.read(readed) != 4) {
			throw new EOFException("Unexpected EOF");
		}

		if(order == ByteOrder.BIG_ENDIAN) {
			return ((((readed[0] & 0xff) << 24) | ((readed[1] & 0xff) << 16) | ((readed[2] & 0xff) <<  8) | (readed[3] & 0xff)) == expected);
		} else {
			final int checksum = (((readed[3] & 0xff) << 24) | ((readed[2] & 0xff) << 16) | ((readed[1] & 0xff) <<  8) | (readed[0] & 0xff));
			return (checksum == expected);
		}
	}
}
