package de.mknjc.apps.jbackup;

import java.util.Arrays;

public class StackedArrayIndexCache implements IndexCache {
	private Object[] chunks;
	private int chunkCount;
	private int mask;

	public StackedArrayIndexCache(ChunkID[] ids) {
		this.chunkCount = ids.length;
		int i;
		for(i = 26; i < 31; i++) {
			if((1 << i) > this.chunkCount) {
				break;
			}
		}

		this.chunks = new Object[1<<i];
		mask = (this.chunks.length - 1);

		for (final ChunkID chunkID : ids) {
			insertChunk(chunkID, this.chunks);
		}
		//printHistogram();
	}

	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCache#hasChunk(long)
	 */
	@Override
	public boolean hasChunk(long rollingHash) {
		final Object bucket = this.chunks[(int)rollingHash & mask];
		if(bucket == null)
			return false;
		if(bucket instanceof ChunkID) {
			ChunkID chunk = ((ChunkID)bucket);
			return chunk.getRollingHash() == rollingHash ? true : false;
		}
		for (final ChunkID c : ((ChunkID[])bucket)) {
			if(c.getRollingHash() == rollingHash)
				return true;
		}

		return false;
	}
	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCache#hasChunkWithHash(long, long, long)
	 */
	@Override
	public ChunkID hasChunkWithHash(long rollingHash, long shaHash0, long shaHash1) {
		final Object bucket = this.chunks[(int)rollingHash & mask];
		if(bucket == null)
			return null;
		if(bucket instanceof ChunkID)
			return ((ChunkID)bucket).getRollingHash() == rollingHash && ((ChunkID)bucket).getHash0() == shaHash0 && ((ChunkID)bucket).getHash1() == shaHash1 ? ((ChunkID)bucket) : null;

			for (final ChunkID c : ((ChunkID[])bucket)) {
				if(c.getRollingHash() == rollingHash && c.getHash0() == shaHash0 && c.getHash1() == shaHash1)
					return c;
			}
			return null;
	}

	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCache#addChunk(de.mknjc.apps.jbackup.ChunkID)
	 */
	@Override
	public void addChunk(ChunkID id) {
		insertChunk(id, this.chunks);
		this.chunkCount++;

		if(this.chunkCount > this.chunks.length && this.chunks.length != 1<<30) {
			//throw new RuntimeException();
			System.err.println("Chunk resize " + this.chunkCount);

			Object[] newChunks = new Object[this.chunks.length << 1];

			for (Object c : chunks) {
				if(c == null)
					continue;
				if(c instanceof ChunkID) {
					insertChunk((ChunkID)c, newChunks);
				} else {
					ChunkID[] bucket = ((ChunkID[])c);
					for (ChunkID chunkID : bucket) {
						insertChunk(chunkID, newChunks);
					}
				}
			}
			this.chunks = newChunks;
			this.mask = (chunks.length - 1);
		}
	}
	@Override
	public long count() {
		return chunkCount;
	}

	private static void insertChunk(ChunkID id, Object[] chunks) {
		Object bucket = chunks[(int)id.getRollingHash() & (chunks.length - 1)];
		if(bucket == null)
			bucket = id;
		else if(bucket instanceof ChunkID) {
			ChunkID oldID = (ChunkID)bucket;
			bucket = new ChunkID[2];
			((ChunkID[])bucket)[0] = oldID;
			((ChunkID[])bucket)[1] = id;
		} else {
			bucket = Arrays.copyOf(((ChunkID[])bucket), ((ChunkID[])bucket).length + 1);
			((ChunkID[])bucket)[((ChunkID[])bucket).length - 1] = id;
		}

		chunks[(int)id.getRollingHash() & (chunks.length - 1)] = bucket;
	}
}
