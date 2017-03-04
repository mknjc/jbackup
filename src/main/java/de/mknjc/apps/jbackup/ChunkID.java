package de.mknjc.apps.jbackup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;

public final class ChunkID {
	private final long rollingHash;
	private final int size;
	private final long hash0;
	private final long hash1;
	private final byte[] bundleID;


	public ChunkID(final long rollingHash, final int size, final byte[] hash, final byte[] bundleID) {
		this.rollingHash = rollingHash;
		this.size = size;

		this.hash0 = ChunkID.makeLongFromArray(hash, 0);
		this.hash1 = ChunkID.makeLongFromArray(hash, 8);

		this.bundleID = bundleID;
	}
	public ChunkID(final long rollingHash, final int size, final long hash0, final long hash1, final byte[] bundleID) {
		this.rollingHash = rollingHash;
		this.size = size;
		this.hash0 = hash0;
		this.hash1 = hash1;
		this.bundleID = bundleID;
	}
	public ChunkID(final ByteString id, final int size) {
		final ByteBuffer bb = id.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
		this.hash0 = bb.getLong();
		this.hash1 = bb.getLong();
		this.rollingHash = bb.getLong();

		this.size = size;

		this.bundleID = null;
	}

	public long getRollingHash() {
		return this.rollingHash;
	}
	public int getSize() {
		return this.size;
	}
	public long getHash0() {
		return this.hash0;
	}
	public long getHash1() {
		return this.hash1;
	}

	public byte[] getBundleID() {
		return this.bundleID;
	}

	@Override
	public boolean equals(final Object obj) {
		if(obj == this)
			return true;

		if(obj == null || !(obj instanceof ChunkID))
			return false;

		final ChunkID other = (ChunkID)obj;

		return this.rollingHash == other.rollingHash && this.hash0 == other.hash0 && this.hash1 == other.hash1 && this.size == other.size;
	}
	@Override
	public int hashCode() {
		return Long.hashCode(this.rollingHash) ^ Long.hashCode(this.hash0) ^ Long.hashCode(this.hash1) ^ this.size;
	}

	@Override
	public String toString() {
		return String.format("Size: %d Rolling Hash: 0x%016x SHAHash: 0x%016x%016x", this.size, this.rollingHash, this.hash0, this.hash1);
	}

	public byte[] getHashArray() {
		final byte[] idArray = new byte[16];
		idArray[0]  = (byte)( this.hash0              & 0xffL);
		idArray[1]  = (byte)((this.hash0       >>  8) & 0xffL);
		idArray[2]  = (byte)((this.hash0       >> 16) & 0xffL);
		idArray[3]  = (byte)((this.hash0       >> 24) & 0xffL);
		idArray[4]  = (byte)((this.hash0       >> 32) & 0xffL);
		idArray[5]  = (byte)((this.hash0       >> 40) & 0xffL);
		idArray[6]  = (byte)((this.hash0       >> 48) & 0xffL);
		idArray[7]  = (byte)((this.hash0       >> 56) & 0xffL);
		idArray[8]  = (byte)( this.hash1              & 0xffL);
		idArray[9]  = (byte)((this.hash1       >>  8) & 0xffL);
		idArray[10] = (byte)((this.hash1       >> 16) & 0xffL);
		idArray[11] = (byte)((this.hash1       >> 24) & 0xffL);
		idArray[12] = (byte)((this.hash1       >> 32) & 0xffL);
		idArray[13] = (byte)((this.hash1       >> 40) & 0xffL);
		idArray[14] = (byte)((this.hash1       >> 48) & 0xffL);
		idArray[15] = (byte)((this.hash1       >> 56) & 0xffL);

		return idArray;
	}
	public ByteString toByteString() {
		final Output out = ByteString.newOutput(24);

		out.write((byte)( this.hash0              & 0xffL));
		out.write((byte)((this.hash0       >>  8) & 0xffL));
		out.write((byte)((this.hash0       >> 16) & 0xffL));
		out.write((byte)((this.hash0       >> 24) & 0xffL));
		out.write((byte)((this.hash0       >> 32) & 0xffL));
		out.write((byte)((this.hash0       >> 40) & 0xffL));
		out.write((byte)((this.hash0       >> 48) & 0xffL));
		out.write((byte)((this.hash0       >> 56) & 0xffL));
		out.write((byte)( this.hash1              & 0xffL));
		out.write((byte)((this.hash1       >>  8) & 0xffL));
		out.write((byte)((this.hash1       >> 16) & 0xffL));
		out.write((byte)((this.hash1       >> 24) & 0xffL));
		out.write((byte)((this.hash1       >> 32) & 0xffL));
		out.write((byte)((this.hash1       >> 40) & 0xffL));
		out.write((byte)((this.hash1       >> 48) & 0xffL));
		out.write((byte)((this.hash1       >> 56) & 0xffL));
		out.write((byte)( this.rollingHash        & 0xffL));
		out.write((byte)((this.rollingHash >>  8) & 0xffL));
		out.write((byte)((this.rollingHash >> 16) & 0xffL));
		out.write((byte)((this.rollingHash >> 24) & 0xffL));
		out.write((byte)((this.rollingHash >> 32) & 0xffL));
		out.write((byte)((this.rollingHash >> 40) & 0xffL));
		out.write((byte)((this.rollingHash >> 48) & 0xffL));
		out.write((byte)((this.rollingHash >> 56) & 0xffL));

		return out.toByteString();
	}

	static long makeLongFromArray(final byte[] arr, final int offset) {
		return ((((long)arr[offset + 0]       ) << 56) |
				(((long)arr[offset + 1] & 0xff) << 48) |
				(((long)arr[offset + 2] & 0xff) << 40) |
				(((long)arr[offset + 3] & 0xff) << 32) |
				(((long)arr[offset + 4] & 0xff) << 24) |
				(((long)arr[offset + 5] & 0xff) << 16) |
				(((long)arr[offset + 6] & 0xff) <<  8) |
				(((long)arr[offset + 7] & 0xff)      ));
	}

}
