package de.mknjc.apps.jbackup;

public class LinearArrayIndexCache implements IndexCache {
	private ChunkID[] chunks;
	private int chunkCount;
	private int mask;

	public LinearArrayIndexCache(ChunkID[] ids) {
		this.chunkCount = ids.length;
		int i;
		for(i = 20; i < 31; i++) {
			if((1 << i) > this.chunkCount) {
				break;
			}
		}

		this.chunks = new ChunkID[1<<i];
		mask = (this.chunks.length - 1);

		for (final ChunkID chunkID : ids) {
			int idx = (int)chunkID.getRollingHash() & mask;
			while(chunks[idx] != null) {
				idx = (idx + 1) & mask;
			}

			chunks[idx] = chunkID;
		}
		//printHistogram();
	}

	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCacheI#hasChunk(long)
	 */
	@Override
	public boolean hasChunk(long rollingHash) {
		int idx = (int)rollingHash & mask;
		ChunkID id;
		while((id = chunks[idx]) != null) {
			if(id.getRollingHash() == rollingHash)
				return true;

			idx = (idx + 1) & mask;
		}
		return false;
	}
	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCacheI#hasChunkWithHash(long, long, long)
	 */
	@Override
	public ChunkID hasChunkWithHash(long rollingHash, long shaHash0, long shaHash1) {
		int idx = (int)rollingHash & mask;
		ChunkID id;
		while((id = chunks[idx]) != null) {
			if(id.getRollingHash() == rollingHash && id.getHash0() == shaHash0 && id.getHash1() == shaHash1)
				return id;

			idx = (idx + 1) & mask;
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCacheI#addChunk(de.mknjc.apps.jbackup.ChunkID)
	 */
	@Override
	public void addChunk(ChunkID id) {
		int idx = (int)id.getRollingHash() & mask;
		while(chunks[idx] != null) {
			idx = (idx + 1) & mask;
		}

		chunks[idx] = id;

		this.chunkCount++;

		if(this.chunkCount > this.chunks.length && this.chunks.length != 1<<30) {
			System.err.println("Chunk resize " + this.chunkCount);
			final ChunkID[] newChunks = new ChunkID[this.chunks.length<<1];
			int newMask = (newChunks.length - 1);

			for (final ChunkID chunkID : this.chunks) {
				int newIdx = (int)chunkID.getRollingHash() & newMask;
				while(newChunks[newIdx] != null) {
					newIdx = (newIdx + 1) & newMask;
				}
				newChunks[newIdx] = chunkID;
			}
			this.chunks = newChunks;
			this.mask = newMask;
		}
	}

	@Override
	public long count() {
		return chunkCount;
	}
}

