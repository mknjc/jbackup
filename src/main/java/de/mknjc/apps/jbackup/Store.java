package de.mknjc.apps.jbackup;

import java.io.IOException;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;

public interface Store {
	public ChunkID hasChunk( long rollingHash, int size, byte[] chunkID);

	public ChunkID saveChunk(byte[] chunk, int offset, int length, byte[] chunkID, long rollingHash);

	public void finish() throws IOException;


	public int getMaxChunkSize();

	Future<Bundle> loadBundle(byte[] id);

	ChunkID lookupID(final ByteString bs);

}
