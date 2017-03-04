package de.mknjc.apps.jbackup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;
import de.mknjc.apps.zbackup.proto.Zbackup.BackupInfo;
import de.mknjc.apps.zbackup.proto.Zbackup.BackupInstruction;

public class Main {


	private static void Usage() {

	}



	public static void main(final String[] args) throws InterruptedException, NoSuchAlgorithmException, IOException {
		boolean actionBackup = false;
		boolean actionRestore = false;
		Path backupPath = null;
		final List<String> storeConfig   = new ArrayList<>();
		final List<String> runtimeConfig = new ArrayList<>();

		for (int i = 0; i < args.length; i++) {

			switch (args[i]) {
			case "backup":
				actionBackup = true;
				backupPath = Paths.get(args[++i]).toAbsolutePath().normalize();
				break;
			case "restore":
				actionRestore = true;
				backupPath = Paths.get(args[++i]).toAbsolutePath().normalize();
				break;
			case "-o":
				storeConfig.add(args[++i]);
				break;
			case "-O":
				runtimeConfig.add(args[++i]);
				break;
			default:
				Main.Usage();
				return;
			}
		}

		if(!(actionBackup || actionRestore)) {
			Main.Usage();
			return;
		}

		if(actionBackup) {
			final Path storePath = Main.getStorePath(backupPath);

			final ZbackupStore store = new ZbackupStore(storePath, storeConfig, runtimeConfig);
			final BackupCreator bc = new BackupCreator(System.in, store);
			bc.run();

			ByteString instructions = bc.getBackupInstructions();
			int instructionLength = instructions.size();
			int iterations = 0;

			while(true) {
				iterations++;
				final BackupCreator instructionpacker = new BackupCreator(instructions.newInput(), store);
				instructionpacker.run();
				instructions = instructionpacker.getBackupInstructions();
				if(instructions.size() >= instructionLength)
					break;
				instructionLength = instructions.size();
			}

			store.writeInstructions(instructions, bc.getInputLength(), bc.getSha256Hash(), iterations, backupPath);

			store.finish();
		}
		if(actionRestore) {
			final Path storePath = Main.getStorePath(backupPath);

			final ZbackupStore store = new ZbackupStore(storePath, storeConfig, runtimeConfig);

			final BackupInfo info = store.readInstructions(backupPath);

			InputStream instStream = new ByteBufferInputStream(info.getBackupData().asReadOnlyByteBuffer());

			int iterations = info.getIterations();
			while(iterations > 0) {
				final ArrayList<BackupInstruction> instructions = new ArrayList<>();
				while(instStream.available() > 0) {
					instructions.add(BackupInstruction.parseDelimitedFrom(instStream));
				}

				final ByteArrayOutputStream os = new ByteArrayOutputStream();
				BackupRestorer.restore(instructions, store, os, 16);
				instStream = new ByteArrayInputStream(os.toByteArray());
				iterations--;
			}

			final ArrayList<BackupInstruction> instructions = new ArrayList<>();
			while(instStream.available() > 0) {
				instructions.add(BackupInstruction.parseDelimitedFrom(instStream));
			}

			final SHA256OutputStream checkedOs = new SHA256OutputStream(System.out);

			BackupRestorer.restore(instructions, store, checkedOs, 16);

			store.finish();

			if(info.getSize() != checkedOs.getLength()) {
				throw new RuntimeException("Length doesn't match");
			}

			if(!Arrays.equals(info.getSha256().toByteArray(), checkedOs.getChecksum())) {
				throw new RuntimeException("Checksum doesn't match");
			}

		}


		//Thread.sleep(200000);
	}



	private static Path getStorePath(final Path backupPath) {
		Path storePath = backupPath;
		while(true) {
			if(storePath == null)
				throw new IllegalArgumentException(backupPath + " is not in a zbackup store");

			final Path fileName = storePath.getFileName();
			if(fileName == null)
				throw new IllegalArgumentException(backupPath + " is not in a zbackup store");
			if(fileName.toString().equals("backups")) {
				storePath = storePath.getParent();
				break;
			}

			storePath = storePath.getParent();
		}
		return storePath;
	}

}
