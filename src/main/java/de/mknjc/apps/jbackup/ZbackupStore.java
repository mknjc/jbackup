package de.mknjc.apps.jbackup;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import com.google.protobuf.ByteString;
import de.mknjc.apps.zbackup.proto.Zbackup.BackupInfo;
import de.mknjc.apps.zbackup.proto.Zbackup.BundleInfo;
import de.mknjc.apps.zbackup.proto.Zbackup.ExtendedStorageInfo;
import de.mknjc.apps.zbackup.proto.Zbackup.FileHeader;

public class ZbackupStore implements Store {

	private Bundle currentBundle;

	private final HashMap<Path, BundleInfo> newBundles = new HashMap<>();

	private final ThreadPoolExecutor executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(128), new ThreadPoolExecutor.CallerRunsPolicy());
	private final Semaphore bundleWriterSem = new Semaphore(Runtime.getRuntime().availableProcessors());

	private final Path store;

	private final Random rand = new Random();

	private final Config config = new Config();

	private final IndexCache indexCache;

	public ZbackupStore(final Path store, final List<String> storeOptions, final List<String> runtimeOptions) throws IOException {
		this.store = store;

		if(Files.isRegularFile(store)) {
			try(InputStream is = Files.newInputStream(store.resolve("info_extended"), StandardOpenOption.READ)) {
				final ExtendedStorageInfo info = ExtendedStorageInfo.parseDelimitedFrom(is);
				this.config.bundleCompressionMethod = info.getConfig().getBundle().getCompressionMethod();
				this.config.bundleMaxPayload = info.getConfig().getBundle().getMaxPayloadSize();
				this.config.chunkMaxSize = info.getConfig().getChunk().getMaxSize();
				this.config.compressionLevel = info.getConfig().getLzma().getCompressionLevel();
			}
		}

		for (final String option : storeOptions) {
			final int eq = option.indexOf('=');
			final String key = eq > 0 ? option.substring(0, eq) : option;
			final String val = eq > 0 && eq < option.length() - 1 ? option.substring(eq + 1, option.length()) : null;

			switch (key) {
			case "chunk.max_size":
				this.config.chunkMaxSize = parseSize(val);
				break;
			case "bundle.max_payload_size":
				this.config.bundleMaxPayload = parseSize(val);
				break;
			case "compression":
			case "bundle.compression_method":
				this.config.bundleCompressionMethod = val;
				break;
			case "lzma.compression_level":
				this.config.compressionLevel = Integer.parseInt(val);
				break;
			case "bundle.erasure_level":
				this.config.erasureLevel = Integer.parseInt(val);
				break;

			default:
				System.err.println("Unknown config " + option);
				break;
			}
		}
		for (String option : runtimeOptions) {
			final int eq = option.indexOf('=');
			final String key = eq > 0 ? option.substring(0, eq) : option;
			final String val = eq > 0 && eq < option.length() - 1 ? option.substring(eq + 1, option.length()) : null;

			switch (key) {
			case "cache-size":
				this.config.chunksInCache = Integer.parseInt(val);
				break;

			default:
				break;
			}
		}

		Files.createDirectories(store.resolve("index"));
		final ChunkID[] loadedChunks;
		try (Stream<Path> files = Files.list(store.resolve("index"))) {
			loadedChunks = files
					.flatMap(t -> ExceptionHelper.runtime(IndexFile::getChunksInIndex, t))
					.toArray(i -> new ChunkID[i]);
		}

		indexCache = new StackedArrayIndexCache(loadedChunks);
	}

	@Override
	public ChunkID lookupID(final ByteString bs) {
		final ByteBuffer bb = bs.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
		final long hash0 = bb.getLong();
		final long hash1 = bb.getLong();
		final long rollingHash = bb.getLong();

		return indexCache.hasChunkWithHash(rollingHash, hash0, hash1);
	}

	@Override
	public Future<Bundle> loadBundle(final byte[] id) {
		return this.executor.submit(() -> {
			return Bundle.readBundleFromFile(
					this.store
					.resolve("bundles")
					.resolve(String.format("%02x", id[0]))
					.resolve(ZbackupStore.bundleString(id)));
		});
	}


	@Override
	public ChunkID hasChunk(final long rollingHash, final int size, final byte[] shaHash) {
		if(shaHash != null) {
			long hash0 = ChunkID.makeLongFromArray(shaHash, 0);
			long hash1 = ChunkID.makeLongFromArray(shaHash, 8);
			return indexCache.hasChunkWithHash(rollingHash, hash0, hash1);
		} else {
			return indexCache.hasChunk(rollingHash);
		}

	}

	@Override
	public ChunkID saveChunk(final byte[] chunk, final int offset, final int length, final byte[] shahash, final long rollingHash) {
		final ChunkID id = new ChunkID(rollingHash, length, shahash, null);

		indexCache.addChunk(id);

		if(this.currentBundle == null) {
			this.currentBundle = new Bundle(this.config.bundleMaxPayload);
		}
		if(!this.currentBundle.addChunk(chunk, offset, length, id)) {
			this.saveBundle();
			this.currentBundle = new Bundle(this.config.bundleMaxPayload);
			if(!this.currentBundle.addChunk(chunk, offset, length, id)) {
				throw new Error("a newly created bundle has not enough size for this chunk");
			}
		}

		return id;

	}

	@Override
	public void finish() throws IOException {
		this.saveBundle();
		this.executor.shutdown();
		while(!this.executor.isTerminated()) {
			try {
				this.executor.awaitTermination(1, TimeUnit.DAYS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		final Map<byte[], BundleInfo> indexBundles = new HashMap<>();
		for (final Entry<Path, BundleInfo> bundle : this.newBundles.entrySet()) {
			final byte[] bID = this.generateBundleId();

			indexBundles.put(bID, bundle.getValue());

			final Path bundleDir = this.store.resolve("bundles").resolve(String.format("%02x", bID[0]));

			Files.createDirectories(bundleDir);
			Files.move(bundle.getKey(), bundleDir.resolve(ZbackupStore.bundleString(bID)));
		}

		if(!indexBundles.isEmpty()) {
			final byte[] iID = this.generateBundleId();
			IndexFile.writeIndex(this.store.resolve("index").resolve(ZbackupStore.bundleString(iID)), indexBundles);
		}
	}

	public void writeInstructions(final ByteString instr, final long length, final ByteString sha256Hash, final int iterations, final Path backupPath) throws IOException {
		final FileHeader header = FileHeader.newBuilder().setVersion(1).build();
		final BackupInfo info = BackupInfo.newBuilder().setSize(length).setSha256(sha256Hash).setIterations(iterations).setBackupData(instr).build();
		Files.createDirectories(backupPath.getParent());

		try(OutputStream os = Files.newOutputStream(backupPath, StandardOpenOption.CREATE_NEW)) {
			final Adler32OutputStream cos = new Adler32OutputStream(os);
			header.writeDelimitedTo(cos);
			info.writeDelimitedTo(cos);

			cos.writeChecksum(ByteOrder.LITTLE_ENDIAN);
			cos.flush();
		}
	}

	public BackupInfo readInstructions(final Path path) throws IOException {
		try(final InputStream is = Files.newInputStream(path, StandardOpenOption.READ)) {
			final Adler32InputStream cis = new Adler32InputStream(is);

			if(FileHeader.parseDelimitedFrom(cis).getVersion() != 1) {
				throw new IOException("Invalid file version");
			}

			final BackupInfo info = BackupInfo.parseDelimitedFrom(cis);

			if(!cis.readChecksum(ByteOrder.LITTLE_ENDIAN))
				throw new IOException("Checksum error");
			return info;
		}
	}



	static String bundleString(final byte[] bundleID) {
		return String.format("%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
				bundleID[0],  bundleID[1],  bundleID[2],  bundleID[3],  bundleID[4],  bundleID[5],  bundleID[6],  bundleID[7],
				bundleID[8],  bundleID[9],  bundleID[10], bundleID[11], bundleID[12], bundleID[13], bundleID[14], bundleID[15],
				bundleID[16], bundleID[17], bundleID[18], bundleID[19], bundleID[20], bundleID[21], bundleID[22], bundleID[23]
				);
	}



	private void saveBundle() {
		if(this.currentBundle == null)
			return;

		final Bundle workbundle = this.currentBundle;
		this.currentBundle = null;

		// we block here so not to much Bundles get created
		this.bundleWriterSem.acquireUninterruptibly();

		final Runnable run = () -> {
			OutputStream stream;
			Path tmpPath;

			try {
				while(true) {
					final String tmpName = this.generateRandomName();
					tmpPath = this.store.resolve(tmpName);
					try {
						stream = new BufferedOutputStream(Files.newOutputStream(tmpPath, StandardOpenOption.CREATE_NEW));
						break;
					} catch (final IOException e) {
						e.printStackTrace();
					}
				}
				final BundleInfo bInfo = workbundle.writeTo(stream, this.config.bundleCompressionMethod, this.config.compressionLevel, this.config.erasureLevel);
				stream.close();

				synchronized (this.newBundles) {
					this.newBundles.put(tmpPath, bInfo);
				}
			} catch (final IOException e) {
				e.printStackTrace();
			} finally {
				this.bundleWriterSem.release();
			}

		};
		this.executor.execute(run);
	}

	private byte[] generateBundleId() {
		final byte[] bundleID = new byte[24];

		this.rand.nextBytes(bundleID);

		return bundleID;
	}

	private int parseSize(String value) {
		Matcher m = Pattern.compile("([0-9]+)([kKmMgG]?)[bB]?").matcher(value);

		if(!m.matches())
			throw new IllegalArgumentException(value + " is a invalid size string");

		int size = Integer.parseInt(m.group(1));

		String mod = m.group(2);
		if(mod == null || mod.length() == 0)
			return size;

		switch(mod.charAt(0)) {
		case 'k':
		case 'K':
			return size * 1024;
		case 'm':
		case 'M':
			return size * 1024 * 1024;
		case 'g':
		case 'G':
			return size * 1024 * 1024 * 1024;
		default:
			throw new IllegalArgumentException("");
		}
	}

	private String generateRandomName() {
		final int[] chars = this.rand.ints(0x41, 0x7b).filter(i -> (i < 0x5b) || (i > 0x60)).limit(8).toArray();
		return new String(chars, 0, 8);
	}

	private static final class Config {
		int chunkMaxSize = 1 << 16;
		int bundleMaxPayload = 1 << 21;
		String bundleCompressionMethod = "lzma";
		int compressionLevel = 6;
		int erasureLevel = 0;
		int chunksInCache = 16;
	}

	@Override
	public int getMaxChunkSize() {
		return this.config.chunkMaxSize;
	}

	@Override
	public int getMaxChunksInCache() {
		return this.config.chunksInCache;
	}
}
