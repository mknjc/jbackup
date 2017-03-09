package de.mknjc.apps.jbackup;

public interface IndexCache {

	ChunkID hasChunk(long rollingHash);

	ChunkID hasChunkWithHash(long rollingHash, long shaHash0, long shaHash1);

	void addChunk(ChunkID id);

	long count();

}