package de.mknjc.apps.jbackup;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import com.google.protobuf.ByteString;

import de.mknjc.apps.zbackup.proto.Zbackup.BundleInfo;
import de.mknjc.apps.zbackup.proto.Zbackup.BundleInfo.ChunkRecord;
import de.mknjc.apps.zbackup.proto.Zbackup.FileHeader;
import de.mknjc.apps.zbackup.proto.Zbackup.IndexBundleHeader;

public class IndexFile {
	public static final int INDEX_FILE_VERSION = 1;


	public static void writeIndex(final Path indexFile, final Map<byte[], BundleInfo> bundles) throws IOException {
		try(final Adler32OutputStream cos = new Adler32OutputStream(Files.newOutputStream(indexFile, StandardOpenOption.CREATE_NEW))) {

			final FileHeader header = FileHeader.newBuilder().setVersion(IndexFile.INDEX_FILE_VERSION).build();
			header.writeDelimitedTo(cos);

			for (final Entry<byte[], BundleInfo> bundle : bundles.entrySet()) {
				final IndexBundleHeader ibh = IndexBundleHeader.newBuilder().setId(ByteString.copyFrom(bundle.getKey())).build();
				ibh.writeDelimitedTo(cos);

				bundle.getValue().writeDelimitedTo(cos);
			}

			//end with empty ibh
			final IndexBundleHeader ibh = IndexBundleHeader.newBuilder().build();
			ibh.writeDelimitedTo(cos);

			cos.writeChecksum(ByteOrder.LITTLE_ENDIAN);
		}
	}


	public static Stream<ChunkID> getChunksInIndex(final Path indexFile) throws IOException {
		final IndexFileReader reader = new IndexFileReader(indexFile);

		return StreamSupport.stream(reader, false).onClose(IndexFile.asUncheckedRunnable(reader));
	}


	private static class IndexFileReader extends Spliterators.AbstractSpliterator<ChunkID> implements Closeable {
		private final Adler32InputStream is;
		private BundleInfo currentBundleInfo;
		private int currentBundlePos = 0;
		private byte[] currentBundleId;

		public IndexFileReader(final Path indexFile) throws IOException {
			super(Long.MAX_VALUE, Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.ORDERED);

			try {
				this.is = new Adler32InputStream(Files.newInputStream(indexFile, StandardOpenOption.READ));

				final FileHeader header = FileHeader.parseDelimitedFrom(this.is);
				if(header.hasVersion() && header.getVersion() != IndexFile.INDEX_FILE_VERSION) {
					throw new IOException("Invalid Index file version " + header.getVersion());
				}
			} catch (final IOException e) {
				System.err.println("Error in file " + indexFile);
				throw e;
			}

		}

		@Override
		public boolean tryAdvance(final Consumer<? super ChunkID> action) {
			if(this.currentBundleInfo == null) {
				try {
					final IndexBundleHeader header = IndexBundleHeader.parseDelimitedFrom(this.is);
					if(!header.hasId()) {
						this.is.readChecksum(ByteOrder.LITTLE_ENDIAN);
						this.close();
						return false;
					}
					this.currentBundleId = header.getId().toByteArray();

					this.currentBundleInfo = BundleInfo.parseDelimitedFrom(this.is);
					this.currentBundlePos = 0;
				} catch (final IOException e) {
					e.printStackTrace();
					throw new RuntimeException("Error in io", e);
				}
			}
			final ChunkRecord record = this.currentBundleInfo.getChunkRecord(this.currentBundlePos++);
			if(this.currentBundleInfo.getChunkRecordCount() == this.currentBundlePos)
				this.currentBundleInfo = null;

			final ByteBuffer bb = record.getId().asReadOnlyByteBuffer();
			bb.order(ByteOrder.LITTLE_ENDIAN);
			final long hash0 = bb.getLong();
			final long hash1 = bb.getLong();
			final long rollingHash = bb.getLong();
			final int size = record.getSize();


			action.accept(new ChunkID(rollingHash, size, hash0, hash1, this.currentBundleId));


			return true;
		}
		@Override
		public void close() throws IOException {
			this.is.close();
		}
	}

	private static Runnable asUncheckedRunnable(final Closeable c) {
		return () -> {
			try {
				c.close();
			} catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		};
	}
}
