package de.mknjc.apps.jbackup;

public interface IndexCache {

	boolean hasChunk(long rollingHash);

	ChunkID hasChunkWithHash(long rollingHash, long shaHash0, long shaHash1);

	void addChunk(ChunkID id);

	long count();

}