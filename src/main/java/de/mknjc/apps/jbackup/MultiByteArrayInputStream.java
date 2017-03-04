package de.mknjc.apps.jbackup;

import java.io.IOException;
import java.io.InputStream;

public class MultiByteArrayInputStream extends InputStream {
	private final byte[][] arrays;

	private int arr;
	private int pos;

	public MultiByteArrayInputStream(final byte[][] arrays) {
		this.arrays = arrays;
	}

	@Override
	public int read() throws IOException {
		if(this.arr >= this.arrays.length)
			return -1;

		final int ret = this.arrays[this.arr][this.pos++] & 0xff;
		if(this.pos >= this.arrays[this.arr].length) {

		}
		return ret;
	}

	@Override
	public int read(final byte[] b, final int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException();
		} else if (off < 0 || len < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}

		if(this.arr >= this.arrays.length) {
			return -1;
		}

		final int startLength = len;
		while(len > 0) {
			final int thisRun = Math.min(this.arrays[this.arr].length, len);
			System.arraycopy(this.arrays[this.arr], this.pos, b, off, thisRun);
			len -= thisRun;
			this.pos += thisRun;
			if(this.pos >= this.arrays[this.arr].length) {
				this.arr++; this.pos = 0;
			}

			if(this.arr >= this.arrays.length) {
				return startLength - len;
			}
		}


		return startLength;
	}

}
