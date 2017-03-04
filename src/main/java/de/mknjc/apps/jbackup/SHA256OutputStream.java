package de.mknjc.apps.jbackup;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA256OutputStream extends FilterOutputStream {
	private final MessageDigest digest;
	private long length = 0;

	public SHA256OutputStream(final OutputStream out) {
		super(out);

		try {
			this.digest = MessageDigest.getInstance("SHA-256");
		} catch (final NoSuchAlgorithmException e) {
			throw new Error("SHA-256 is required");
		}
	}

	@Override
	public void write(final int b) throws IOException {
		this.out.write(b);
		this.digest.update((byte)b);
		this.length++;
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {
		this.out.write(b, off, len);
		this.digest.update(b, off, len);
		this.length += len;
	}

	public byte[] getChecksum() {
		return this.digest.digest();
	}
	public long getLength() {
		return this.length;
	}
}
