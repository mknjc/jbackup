package de.mknjc.apps.jbackup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.SingleXZInputStream;
import org.tukaani.xz.XZOutputStream;

import com.backblaze.erasure.OutputInputByteTableCodingLoop;
import com.backblaze.erasure.ReedSolomon;
import com.google.protobuf.ByteString;

import de.mknjc.apps.zbackup.proto.Zbackup.BundleFileHeader;
import de.mknjc.apps.zbackup.proto.Zbackup.BundleInfo;
import de.mknjc.apps.zbackup.proto.Zbackup.BundleInfo.ChunkRecord;

public class Bundle {
	private static final int ERASURE_BLOCKS = 256;

	private final List<ChunkID> chunks;

	private final int BUNDLE_SIZE;

	private final ByteBuffer buffer;

	private Bundle(final ByteBuffer buffer, final List<ChunkID> chunkIDs) {
		this.buffer = buffer;
		this.BUNDLE_SIZE = buffer.limit();

		this.chunks = chunkIDs;
	}

	public Bundle(final int maxSize) {
		this.BUNDLE_SIZE = maxSize;
		this.chunks = new ArrayList<>();

		this.buffer = ByteBuffer.wrap(new byte[this.BUNDLE_SIZE]);
	}

	public boolean addChunk(final byte[] data, final int offset, final int length, final ChunkID id) {
		if(this.buffer.remaining() < length)
			return false;

		if(id.getSize() != length)
			throw new RuntimeException();

		this.chunks.add(id);

		if(length > data.length - offset) {
			this.buffer.put(data, offset, data.length - offset);
			this.buffer.put(data, 0, length - (data.length - offset));
		} else {
			this.buffer.put(data, offset, length);
		}
		return true;
	}

	public boolean hasChunk(final ChunkID id) {
		for (final ChunkID chunkID : this.chunks) {
			if(chunkID.equals(id))
				return true;
		}
		return false;
	}

	public void writeTo(final OutputStream stream, final ChunkID chunkToWrite) throws IOException {
		int offset = 0;
		boolean found = false;
		for (final ChunkID chunkID : this.chunks) {
			if(chunkID.equals(chunkToWrite)) {
				found = true;
				break;
			}
			offset += chunkID.getSize();
		}
		if(!found)
			throw new IllegalArgumentException("Chunk not found");

		stream.write(this.buffer.array(), offset, chunkToWrite.getSize());
	}

	/**
	 * 
	 * @param stream
	 * @param compressor
	 * @param level
	 * @param erasureLevel
	 * @return
	 * @throws IOException
	 */
	public BundleInfo writeTo(final OutputStream stream, final String compressor, final int level, final int erasureLevel) throws IOException {
		final BundleFileHeader.Builder headerBuilder = BundleFileHeader.newBuilder().setVersion(1).setCompressionMethod(compressor);
		if(erasureLevel > 0)
			headerBuilder.setErasureShards(erasureLevel);

		final BundleInfo bundleInfo = this.getBundleInfo();

		OutputStream tempStream;
		if(erasureLevel > 0) {
			tempStream = new ByteArrayOutputStream(this.buffer.position());
		} else {
			tempStream = stream;
		}
		final Adler32OutputStream as = new Adler32OutputStream(tempStream);



		headerBuilder.build().writeDelimitedTo(as);

		bundleInfo.writeDelimitedTo(as);

		as.writeChecksum(ByteOrder.LITTLE_ENDIAN);


		switch (compressor) {
		case "lzma":
			final XZOutputStream os = new XZOutputStream(as, new LZMA2Options(level)); // we can't close this stream because it will close the parent stream too
			os.write(this.buffer.array(), this.buffer.arrayOffset(), this.buffer.position() + this.buffer.arrayOffset());
			os.finish();
			break;
		case "zero":
			as.write(this.buffer.array(), this.buffer.arrayOffset(), this.buffer.position() + this.buffer.arrayOffset());
			break;
		default:
			throw new IllegalArgumentException("Unknown compressor " + compressor);
		}

		as.writeChecksum(ByteOrder.LITTLE_ENDIAN);


		if(erasureLevel > 0) {
			final ByteArrayOutputStream baos = (ByteArrayOutputStream)tempStream;
			final byte[] outputBytes = baos.toByteArray();

			final byte[][] shards = new byte[Bundle.ERASURE_BLOCKS][];

			final int shardCount = Bundle.ERASURE_BLOCKS - erasureLevel;
			final int shardSize = (outputBytes.length / shardCount) + 1;

			for (int i = 0; i < shardCount; i++) {
				if(i * shardSize < outputBytes.length)
					shards[i] = Arrays.copyOfRange(outputBytes, i * shardSize, (i + 1) * shardSize);
				else
					shards[i] = new byte[shardSize];
			}
			for(int i = shardCount; i < Bundle.ERASURE_BLOCKS; i++) {
				shards[i] = new byte[shardSize];
			}
			final ReedSolomon rs = new ReedSolomon(Bundle.ERASURE_BLOCKS - erasureLevel, erasureLevel, new OutputInputByteTableCodingLoop());

			rs.encodeParity(shards, 0, shardSize);

			for (final byte[] shard : shards) {
				stream.write(shard);
			}
			final CRC32 crc = new CRC32();

			for (final byte[] shard : shards) {
				crc.reset();
				crc.update(shard);

				stream.write((int)(crc.getValue()       & 0xff));
				stream.write((int)(crc.getValue() >>  8 & 0xff));
				stream.write((int)(crc.getValue() >> 16 & 0xff));
				stream.write((int)(crc.getValue() >> 24 & 0xff));
			}
		}


		return bundleInfo;
	}

	private BundleInfo getBundleInfo() {
		final BundleInfo.Builder infoBuilder = BundleInfo.newBuilder();

		this.chunks.forEach(c -> {
			final ByteString id = c.toByteString();
			infoBuilder.addChunkRecord(ChunkRecord.newBuilder().setId(id).setSize(c.getSize()).build());
		});

		final BundleInfo info = infoBuilder.build();
		return info;
	}

	public static Bundle readBundleFromFile(final Path file) throws IOException {
		final byte[] data = Files.readAllBytes(file);

		Adler32InputStream is = new Adler32InputStream(new ByteArrayInputStream(data));

		final BundleFileHeader header = BundleFileHeader.parseDelimitedFrom(is);

		if(header.hasErasureShards() && header.getErasureShards() > 0) {
			final ReedSolomon rs = new ReedSolomon(Bundle.ERASURE_BLOCKS - header.getErasureShards(), header.getErasureShards(), new OutputInputByteTableCodingLoop());

			final int shardSize = (data.length - 1024) / Bundle.ERASURE_BLOCKS;
			final int checkSumOffset = data.length - 1024;
			final byte[][] shards = new byte[Bundle.ERASURE_BLOCKS][];
			final boolean[] correct = new boolean[Bundle.ERASURE_BLOCKS];
			final CRC32 crc = new CRC32();
			boolean somethingToRepair = false;

			for (int i = 0; i < Bundle.ERASURE_BLOCKS; i++) {
				shards[i] = Arrays.copyOfRange(data, i * shardSize, (i + 1) * shardSize);

				crc.reset();
				crc.update(shards[i]);


				correct[i] =	data[checkSumOffset + i * 4] == (byte)(crc.getValue()       & 0xff) &&
						data[checkSumOffset + i * 4 + 1] == (byte)(crc.getValue() >>  8 & 0xff) &&
						data[checkSumOffset + i * 4 + 2] == (byte)(crc.getValue() >> 16 & 0xff) &&
						data[checkSumOffset + i * 4 + 3] == (byte)(crc.getValue() >> 24 & 0xff);

				if (!correct[i]) {
					somethingToRepair = true;
					System.err.println("Bundle Shard " + i + " corrupt!");
				}
			}

			if(somethingToRepair) {
				rs.decodeMissing(shards, correct, 0, shardSize);

				for (int i = 0; i < Bundle.ERASURE_BLOCKS; i++) {
					crc.reset();
					crc.update(shards[i]);

					correct[i] =	data[checkSumOffset + i * 4] == (byte)(crc.getValue()       & 0xff) &&
							data[checkSumOffset + i * 4 + 1] == (byte)(crc.getValue() >>  8 & 0xff) &&
							data[checkSumOffset + i * 4 + 2] == (byte)(crc.getValue() >> 16 & 0xff) &&
							data[checkSumOffset + i * 4 + 3] == (byte)(crc.getValue() >> 24 & 0xff);

					if (!correct[i]) {
						throw new IOException("Bundle " + file + " shard " + i + " corrupt after reconstruction.");
					}
				}
				is = new Adler32InputStream(new MultiByteArrayInputStream(Arrays.copyOf(shards, Bundle.ERASURE_BLOCKS - header.getErasureShards())));

				// we have to read the header again so the Stream is up
				BundleFileHeader.parseDelimitedFrom(is);
			}
		}

		final BundleInfo info = BundleInfo.parseDelimitedFrom(is);

		if(!is.readChecksum(ByteOrder.LITTLE_ENDIAN))
			throw new IOException("Checksum is invalid");


		final List<ChunkID> ids = new ArrayList<>(info.getChunkRecordCount());
		int dataSize = 0;
		for(final ChunkRecord record : info.getChunkRecordList()) {
			final int size = record.getSize();
			ids.add(new ChunkID(record.getId(), size));
			dataSize += size;
		}

		final ByteBuffer buff = ByteBuffer.allocate(dataSize);

		final String compressor = header.getCompressionMethod();

		InputStream cis;

		switch (compressor) {
		case "lzma":
			cis = new SingleXZInputStream(is);

			break;
		case "zero":
			cis = is;
			break;
		default:
			throw new IOException("Unknown compressor " + compressor);
		}

		while(buff.hasRemaining()) {
			final int readed = cis.read(buff.array(), buff.position() + buff.arrayOffset(), buff.remaining());
			if(readed == -1)
				throw new IOException("Short read from data");
			buff.position(buff.position() + readed);
		}

		if(compressor.equals("lzma"))
			if(cis.read() != -1)
				throw new IOException("Unknown data at the end of lzma stream");


		if(!is.readChecksum(ByteOrder.LITTLE_ENDIAN))
			throw new IOException("Checksum is invalid");

		buff.flip();
		return new Bundle(buff, ids);
	}
}
