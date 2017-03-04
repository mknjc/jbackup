package de.mknjc.apps.jbackup;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import com.google.protobuf.ByteString;

import de.mknjc.apps.zbackup.proto.Zbackup.BackupInstruction;

public class BackupRestorer {
	public static void restore(final List<BackupInstruction> instructions, final Store store, final OutputStream os, final int maxCachedBundles) throws IOException {
		final List<Action> backupActions = new ArrayList<>();

		for (final BackupInstruction instruction : instructions) {
			ChunkID id = null;
			if(instruction.hasChunkToEmit()) {
				id = store.lookupID(instruction.getChunkToEmit());
				if(id == null)
					throw new RuntimeException("Cannot find chunk");
			}
			final Action a = new Action(Action.Type.Emit, null, id, instruction.getBytesToEmit());
			backupActions.add(a);
		}

		final List<byte[]> bundlesInCache = new ArrayList<>();
		for (int i = 0; i < backupActions.size(); i++) {
			int loadPosition = 0;
			final ChunkID c = backupActions.get(i).chunk;
			if(c == null)
				continue;

			if(bundlesInCache.contains(c.getBundleID()))
				continue;


			if(bundlesInCache.size() >= maxCachedBundles) {
				// check for each bundle the nex used time
				final int[] nextUsed = new int[bundlesInCache.size()];
				Arrays.fill(nextUsed, Integer.MAX_VALUE);

				for (int j = i; j < backupActions.size(); j++) {
					final ChunkID id = backupActions.get(j).chunk;
					if(id != null) {
						final int idx = bundlesInCache.indexOf(id.getBundleID());
						if(idx != -1 && nextUsed[idx] == Integer.MAX_VALUE)
							nextUsed[idx] = j;
					}
				}
				final int maxPos = IntStream.range(1, nextUsed.length).reduce(0, (a,b)->nextUsed[a]<nextUsed[b]? b: a);

				// we found the bundle which get used at the latest
				final byte[] bundleToRemove = bundlesInCache.remove(maxPos);
				//find when it was used last
				for (int j = i - 1; j >= 0; j--) {
					final ChunkID id = backupActions.get(j).chunk;
					if(id != null) {
						if(Arrays.equals(id.getBundleID(), bundleToRemove)) {
							// the action which need the bundle is in j so j+1 gets the unload and j+2 the next load
							backupActions.add(j + 1, new Action(Action.Type.UnloadBundle, bundleToRemove, null, null));
							loadPosition = j + 2;
							i++; // we added a instruction before i so increase i
							break;
						}
					}
				}
				if(loadPosition == 0)
					throw new RuntimeException();
			}

			// if we are here there is space in the cache and we also know where to put the load command
			backupActions.add(loadPosition, new Action(Action.Type.LoadBundle, c.getBundleID(), null, null));
			i++;
			bundlesInCache.add(c.getBundleID());
		}

		final HashMap<byte[], Future<Bundle>> bundleCache = new HashMap<>();

		for (final Action a : backupActions) {
			switch (a.type) {
			case LoadBundle:
				bundleCache.put(a.bundle, store.loadBundle(a.bundle));
				break;
			case UnloadBundle:
				bundleCache.remove(a.bundle);
				break;
			case Emit:
				if(a.chunk != null) {
					Bundle b;
					while(true) {
						try {
							b = bundleCache.get(a.chunk.getBundleID()).get();
							break;
						} catch (final InterruptedException e) {
							// ignore
						} catch (final ExecutionException e) {
							throw new IOException(e);
						}
					}
					b.writeTo(os, a.chunk);
				}
				if(a.data != null) {
					a.data.writeTo(os);
				}
			default:
				break;
			}
			os.flush();

		}




	}


	private static final class Action {
		private final byte[] bundle;
		private final ChunkID chunk;
		private final ByteString data;
		private final Type type;

		Action(final Type type, final byte[] bundle, final ChunkID chunk, final ByteString data) {
			this.type = type;
			this.bundle = bundle;
			this.chunk = chunk;
			this.data = data;
		}
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();

			sb.append(this.type);
			switch (this.type) {
			case Emit:
				if(this.chunk != null) {
					sb.append(" chunk ").append(String.format("0x%08x", this.chunk.getRollingHash())).append(" Bundle: ").append(ZbackupStore.bundleString(this.chunk.getBundleID()));
				}
				if(this.data != null) {
					sb.append(" ").append(this.data.size()).append(" bytes data");
				}
				break;
			case LoadBundle:
			case UnloadBundle:
				sb.append(" bundle ").append(ZbackupStore.bundleString(this.bundle));
			}

			return sb.toString();

		}

		private enum Type {
			LoadBundle,
			UnloadBundle,
			Emit
		}
	}

}
