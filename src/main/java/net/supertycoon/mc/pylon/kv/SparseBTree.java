package net.supertycoon.mc.pylon.kv;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.StampedLock;

/**
 * A B-Tree implementation with sparsely populated array-nodes.
 *
 * All blobs are 2MB each. Header included. Header *always* first block(s); can grow and move data blocks.
 *
 *
 *
 */
public class SparseBTree {

	FileChannel fc;

	StampedLock headerlock;

	public SparseBTree(final Path path) throws IOException {
		this.fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

		if (this.fc.size() == 0) {

		}



	}




}
