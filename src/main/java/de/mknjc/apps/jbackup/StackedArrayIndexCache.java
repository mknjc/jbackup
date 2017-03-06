package de.mknjc.apps.jbackup;

import java.util.Arrays;
import java.util.HashMap;

public class StackedArrayIndexCache implements IndexCache {
	private ChunkID[][] chunks;
	private int chunkCount;
	private int mask;

	public StackedArrayIndexCache(ChunkID[] ids) {
		this.chunkCount = ids.length;
		int i;
		for(i = 20; i < 31; i++) {
			if((1 << i) > this.chunkCount) {
				break;
			}
		}

		this.chunks = new ChunkID[1<<i][];
		mask = (this.chunks.length - 1);

		for (final ChunkID chunkID : ids) {
			ChunkID[] bucket = this.chunks[(int)chunkID.getRollingHash() & mask];
			if(bucket == null)
				bucket = new ChunkID[1];
			else
				bucket = Arrays.copyOf(bucket, bucket.length + 1);

			bucket[bucket.length - 1] = chunkID;

			this.chunks[(int)chunkID.getRollingHash() & mask] = bucket;
		}
		//printHistogram();
	}

	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCacheI#hasChunk(long)
	 */
	@Override
	public ChunkID hasChunk(long rollingHash) {
		final ChunkID[] bucket = this.chunks[(int)rollingHash & mask];
		if(bucket == null)
			return null;

		for (final ChunkID c : bucket) {
			if(c.getRollingHash() == rollingHash)
				return c;
		}

		return null;
	}
	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCacheI#hasChunkWithHash(long, long, long)
	 */
	@Override
	public ChunkID hasChunkWithHash(long rollingHash, long shaHash0, long shaHash1) {
		final ChunkID[] bucket = this.chunks[(int)rollingHash & mask];
		if(bucket == null)
			return null;

		for (final ChunkID c : bucket) {
			if(c.getRollingHash() == rollingHash && c.getHash0() == shaHash0 && c.getHash1() == shaHash1)
				return c;
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see de.mknjc.apps.jbackup.IndexCacheI#addChunk(de.mknjc.apps.jbackup.ChunkID)
	 */
	@Override
	public void addChunk(ChunkID id) {
		ChunkID[] bucket = this.chunks[(int)id.getRollingHash() & mask];
		if(bucket == null)
			bucket = new ChunkID[1];
		else
			bucket = Arrays.copyOf(bucket, bucket.length + 1);


		bucket[bucket.length - 1] = id;

		this.chunks[(int)id.getRollingHash() & mask] = bucket;

		this.chunkCount++;

		if(this.chunkCount > this.chunks.length && this.chunks.length != 1<<30) {
			System.err.println("Chunk resize " + this.chunkCount);
			final ChunkID[][] newChunks = new ChunkID[this.chunks.length<<1][];
			int newMask = (newChunks.length - 1);

			for (final ChunkID[] oldBuckets : this.chunks) {
				if(oldBuckets == null)
					continue;

				for (final ChunkID cid : oldBuckets) {
					ChunkID[] nbucket = newChunks[(int)cid.getRollingHash() & newMask];
					if(nbucket == null)
						nbucket = new ChunkID[1];
					else
						nbucket = Arrays.copyOf(nbucket, nbucket.length + 1);

					nbucket[nbucket.length - 1] = cid;

					newChunks[(int)cid.getRollingHash() & newMask] = nbucket;
				}
			}
			this.chunks = newChunks;
			this.mask = newMask;
		}
	}

	private void printHistogram() {
		HashMap<Integer, Long> histogram = new HashMap<>();

		Arrays.stream(this.chunks)
		.mapToInt(b -> {return b == null? 0 : b.length;})
		.forEach(c -> histogram.merge(c, Long.valueOf(1), (a,b) -> a+b));

		histogram.keySet().stream().sorted().forEachOrdered(k -> System.err.printf("%d -> %d%n", k, histogram.get(k)));
	}
}
