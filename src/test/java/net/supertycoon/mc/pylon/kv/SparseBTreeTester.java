package net.supertycoon.mc.pylon.kv;

import net.supertycoon.mc.pylon.kv.SparseBTree;

import java.io.File;
import java.io.IOException;

public class SparseBTreeTester {

	public static void main(String[] args) throws IOException {
		SparseBTree bTree = new SparseBTree(new File("test.bin").toPath());

	}

}
