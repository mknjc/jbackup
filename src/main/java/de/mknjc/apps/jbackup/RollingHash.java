package de.mknjc.apps.jbackup;

public class RollingHash {
	long factor = 0;
	long nextFactor = 1;
	long value = 0;

	public void rollIn(final byte b) {
		this.factor = this.nextFactor;
		this.nextFactor = ( this.nextFactor << 8 ) + this.nextFactor; // nextFactor *= 257
		this.value = ( this.value << 8 ) + this.value;
		this.value += ((long)b & 0xff);
	}

	public void rotate(final byte in, final byte out) {
		this.value -= ((long)out & 0xff) * this.factor;
		this.value = ( this.value << 8 ) + this.value; // value *= 257
		this.value += ((long)in & 0xff);
	}

	public long digest() {
		return this.value + this.nextFactor;
	}

	public static long digest(final byte[] buff, int offset, int length) {
		final RollingHash tmpHash = new RollingHash();
		while(length-- != 0) {
			tmpHash.rollIn(buff[offset++]);
			if(offset == buff.length) offset = 0;
		}
		return tmpHash.digest();
	}
}
