package de.mknjc.apps.jbackup;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;

import de.mknjc.apps.zbackup.proto.Zbackup.BackupInstruction;

public class BackupCreator implements Runnable {
	public static final int MIN_CHUNK_SIZE = 1 << 8;
	public final int MAX_CHUNK_SIZE;


	private final InputStream in;
	private final Store cs;

	private final MessageDigest chunkHash;
	private final MessageDigest totalSHA;

	private long inputLength = 0;

	private final List<BackupInstruction> instructions = new ArrayList<>();


	RollingHash hash = new RollingHash();
	final byte[] buff;
	int hashHead = 0;
	int hashTail = 0;
	int chunkTail = 0;
	int chunkLength = 0;
	int hashLength = 0;

	long nextChunkHash = 0;


	// stats
	int falsePositives;
	int foundChunks;
	int emittedShortChunks;


	public BackupCreator(final InputStream input, final Store cs) {
		this.in = input;
		this.cs = cs;
		this.MAX_CHUNK_SIZE = this.cs.getMaxChunkSize();
		buff =  new byte[this.MAX_CHUNK_SIZE*4];
		try {
			this.chunkHash = MessageDigest.getInstance("SHA-1");
			this.totalSHA = MessageDigest.getInstance("SHA-256");
		} catch (final NoSuchAlgorithmException e1) {
			e1.printStackTrace();
			throw new Error("SHA-1 and SHA-256 is required.", e1);
		}
	}



	/*

	[][][][][][][][][][][][][][][][]
	  ^           ^           ^
	  cT          hT          hH

	  hH          cT          hT

	  hT          hH          cT


	 */
	@Override
	public void run() {

		try {
			int readed = 0;
			while((readed = this.in.read(buff, hashHead, chunkTail <= hashHead ? buff.length - hashHead : chunkTail - hashHead)) != -1) {
				this.totalSHA.update(buff, hashHead, readed);
				this.inputLength += readed;

				while(readed-- != 0) {
					if(hashLength == this.MAX_CHUNK_SIZE) {
						advanceHash();
					} else {
						readInHash();
					}

					if(hashLength >= this.MAX_CHUNK_SIZE) {
						checkMatch();
					}
				}
			}
			System.out.println("Hashhead: " + hashHead + " HashTail: " + hashTail + " ChunkTail: " + chunkTail + " ChunkLength: " + chunkLength);

			if(chunkLength > 0)
				saveChunk(RollingHash.digest(buff, chunkTail, chunkLength));

			if(hashHead != hashTail) {
				if(hashLength >= BackupCreator.MIN_CHUNK_SIZE) {
					final long partChunkHash = RollingHash.digest(buff, hashTail, hashLength);
					final byte[] chunksha = this.calcSha(buff, hashTail, hashLength);
					this.instructChunkID(this.cs.saveChunk(buff, hashTail, hashLength, chunksha, partChunkHash));
				} else {
					this.instructBytes(buff, hashTail, hashLength);
				}
			}
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}

	private void advanceHash() {
		hash.rotate(buff[hashHead++], buff[hashTail++]);
		chunkLength++;

		if(hashHead == buff.length)
			hashHead = 0;
		if(hashTail == buff.length)
			hashTail = 0;

		if(chunkLength == this.MAX_CHUNK_SIZE) {
			// nextChunkHash could only be used by full chunks
			saveChunk(nextChunkHash);

			nextChunkHash = hash.digest();
			chunkTail = hashTail;
			chunkLength = 0;
		}
	}

	private void readInHash() {
		hash.rollIn(buff[hashHead++]);
		hashLength++;

		if(hashHead == buff.length)
			hashHead = 0;

		if(hashLength == this.MAX_CHUNK_SIZE) {
			nextChunkHash = hash.digest();
		}
	}

	private void checkMatch() {
		if(this.cs.hasChunk(hash.digest())) {

			final byte[] sha = this.calcSha(buff, hashTail, hashLength);
			final ChunkID curr = this.cs.getChunk(hash.digest(), hashLength, sha);
			if(curr != null) {
				foundChunks++;
				if(chunkTail != hashTail) {
					//first push chunk part

					// nextChunkHash could only be used by full chunks
					final long partChunkHash = RollingHash.digest(buff, chunkTail, chunkLength);
					saveChunk(partChunkHash);

					chunkLength = 0;
				}
				this.instructChunkID(curr);
				hash = new RollingHash();
				chunkTail = hashTail = hashHead;
				hashLength = 0;

			} else {
				falsePositives++;
				System.out.println("False positive");
			}
		}
	}



	private void saveChunk(long hash) {
		if(chunkLength < BackupCreator.MIN_CHUNK_SIZE) {
			emittedShortChunks++;
			this.instructBytes(buff, chunkTail, chunkLength);

		} else {
			final byte[] chunksha = this.calcSha(buff, chunkTail, chunkLength);

			final ChunkID prev = this.cs.getChunk(hash, chunkLength, chunksha);
			if(prev != null) {
				foundChunks++;
				this.instructChunkID(prev);
			} else {
				this.instructChunkID(this.cs.saveChunk(buff, chunkTail, chunkLength, chunksha, hash));
			}
		}
	}

	public ByteString getBackupInstructions() throws IOException {
		final ByteString.Output out = ByteString.newOutput();
		for (final BackupInstruction inst : this.instructions) {
			inst.writeDelimitedTo(out);
		}

		return out.toByteString();
	}

	public long getInputLength() {
		return this.inputLength;
	}

	public ByteString getSha256Hash() {
		return ByteString.copyFrom(this.totalSHA.digest());
	}

	public void printStats() {
		System.err.println("Found Chunks: " + foundChunks + " False Positives: " + falsePositives + " Short chunks: " + emittedShortChunks);
	}

	private void instructBytes(final byte[] chunk, final int offset, final int length) {
		ByteString bs;
		if(length > chunk.length - offset) {
			final Output tmp = ByteString.newOutput(length);
			tmp.write(chunk, offset, chunk.length - offset);
			tmp.write(chunk, 0, length - (chunk.length - offset));

			bs = tmp.toByteString();
		} else {
			bs = ByteString.copyFrom(chunk, offset, length);
		}

		final BackupInstruction inst = BackupInstruction.newBuilder().setBytesToEmit(bs).build();
		this.instructions.add(inst);
	}

	private void instructChunkID(final ChunkID id) {
		final BackupInstruction inst = BackupInstruction.newBuilder().setChunkToEmit(id.toByteString()).build();
		this.instructions.add(inst);
	}


	/*
	private static String printSha1(final byte[] sha) {
		return String.format("0x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
				sha[0], sha[1], sha[2], sha[3], sha[4], sha[5], sha[6], sha[7], sha[8], sha[9],
				sha[10], sha[11], sha[12], sha[13], sha[14], sha[15], sha[16], sha[17], sha[18], sha[19]
				);
	}*/

	byte[] calcSha(final byte[] buff, final int offset, final int length) {
		this.chunkHash.reset();
		if(length > buff.length - offset) {
			this.chunkHash.update(buff, offset, buff.length - offset);
			this.chunkHash.update(buff, 0, length - (buff.length - offset));
		} else {
			this.chunkHash.update(buff, offset, length);
		}
		return this.chunkHash.digest();
	}
}
