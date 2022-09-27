package comp90015.idxsrv.filemgr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

/**
 * Manage random access to the blocks in a file.
 * @author aaron
 *
 */
public class FileMgr {
	
	private RandomAccessFile file;
	
	private FileDescr fileDescr;
	
	private MessageDigest blockDigest;
	
	private Set<Integer> blocksRequired;
	
	private Set<Integer> blocksDone;
	
	/**
	 * Open the file and create a file descriptor for it. Only use this initializer when
	 * the file is known to be complete.
	 * @param filename the name of the file to open.
	 * @throws NoSuchAlgorithmException when the MD5 hash algorithm is unavailable.
	 * @throws IOException when the file cannot be accessed.
	 */
	public FileMgr(String filename) throws NoSuchAlgorithmException, IOException {
		init(filename);
		fileDescr = new FileDescr(file);
		checkBlocksRequired();
		if(!isComplete()) { // sanity check
			throw new IOException();
		}
	}
	
	/**
	 * Open the file and use the provided file descriptor. This initializer is used when
	 * the file is incomplete, or complete and matches the file descriptor. The existing
	 * file's length will immediately be set to that of the descriptor. If the file contains
	 * blocks that are invalid then those blocks will be set as required, i.e. the initializer
	 * can be called on a file with any content with the intention that the file's complete
	 * content will eventually match the descriptor.
	 * @param filename the name of the file to open.
	 * @param fileDescr the known descriptor of the file.
	 * @throws IOException when the file cannot be accessed.
	 * @throws NoSuchAlgorithmException when the MD5 hash algorithm is unavailable.
	 */
	public FileMgr(String filename, FileDescr fileDescr) throws IOException, NoSuchAlgorithmException {
		init(filename);
		file.setLength(fileDescr.getFileLength());
		this.fileDescr = fileDescr;
		checkBlocksRequired();
	}
	
	private void init(String filename) throws FileNotFoundException, NoSuchAlgorithmException {
		file = new RandomAccessFile(filename, "rw");
		blockDigest = MessageDigest.getInstance("MD5");
		blocksRequired = new HashSet<Integer>();
		blocksDone = new HashSet<Integer>();
	}
	
	private void checkBlocksRequired() throws IOException {
		blocksRequired.clear();
		blocksDone.clear();
		for(int b=0;b<fileDescr.getNumBlocks();b++) {
			byte[] blockBytes = _readBlock(b);
			if(checkBlockHash(b,blockBytes)) {
				blocksDone.add(b);
			} else {
				blocksRequired.add(b);
			}
		}
	}
	
	/**
	 * Return true if the MD5 hash of the given block's bytes match the
	 * MD5 hash of the descriptor at the given block index. Return false
	 * otherwise. Use this method to test that the block is correct before
	 * calling {@link writeBlock}.
	 * @param blockIdx the index of the block, which must be less than the number of blocks
	 * @param blockBytes the bytes of the block
	 * @return true if matched, false otherwise
	 */
	public boolean checkBlockHash(int blockIdx, byte[] blockBytes) {
		blockDigest.update(blockBytes);
		String blockMd5 = FileDescr.bytesToHex(blockDigest.digest());
		blockDigest.reset();
		return blockMd5.equals(fileDescr.getBlockMd5(blockIdx));
	}
	
	/**
	 * Write the block bytes to the file at the given block index. If the block
	 * is not required (it is already present in the file) or the block's MD5 hash
	 * does not match the descriptor then the block is not written. Use {@link checkBlockHash}
	 * before calling this function to check if the block is correct.
	 * @param blockIdx the index of the block, which must be less than the number of blocks
	 * @param blockBytes the bytes of the block
	 * @return true if the block was written, false otherwise
	 * @throws IOException if the file cannot be accessed
	 */
	public boolean writeBlock(int blockIdx, byte[] blockBytes) throws IOException {
		if(blocksRequired.contains(blockIdx) && checkBlockHash(blockIdx,blockBytes)) {
			long offset = fileDescr.getBlockOffset(blockIdx);
			file.seek(offset);
			file.write(blockBytes,0,blockBytes.length);
			blocksRequired.remove(blockIdx);
			blocksDone.add(blockIdx);
			return true;
		} else {
			return false;
		}
	}
	
	private byte[] _readBlock(int blockIdx) throws IOException {
		int numBytes = fileDescr.getNumBlockBytes(blockIdx);
		long offset = fileDescr.getBlockOffset(blockIdx);
		byte[] blockBytes = new byte[(int)numBytes];
		file.seek(offset);
		file.readFully(blockBytes,0,numBytes);
		return blockBytes;
	}
	
	/**
	 * Read the block's bytes from the file. Use {@link isBlockAvailable} to check
	 * if the block is available prior to calling this method.
	 * @param blockIdx the index of the block, which must be less than the number of blocks
	 * @return the block's bytes
	 * @throws IOException if the file cannot be accessed
	 * @throws BlockUnavailableException if the file does not contain the bytes for the block at that block index, i.e. the file is incomplete
	 */
	public byte[] readBlock(int blockIdx) throws IOException, BlockUnavailableException {
		if(blocksDone.contains(blockIdx)){
			return _readBlock(blockIdx);
		}
		throw new BlockUnavailableException();
	}
	
	/**
	 * Check if a block is available.
	 * @param blockIdx the index of the block
	 * @return true if the block is available, false otherwise
	 */
	public boolean isBlockAvailable(int blockIdx) {
		return blocksDone.contains(blockIdx);
	}
	
	/**
	 * Check if all blocks of the file are available.
	 * @return true if all blocks of the file are available, i.e. the file is complete, false otherwise
	 */
	public boolean isComplete() {
		return (blocksDone.size() == fileDescr.getNumBlocks());
	}
	
	/**
	 * Check if the file's MD5 hash matches the descriptor. Call this method as a sanity
	 * check of the the overall file's content, after calling {@link isComplete} to check
	 * if all the blocks are correct.
	 * @return true if the file's MD5 hash matches the descriptor, false otherwise
	 * @throws NoSuchAlgorithmException if the MD5 algorithm is unavailable
	 * @throws IOException if the file cannot be accessed
	 */
	public boolean checkFileHash() throws NoSuchAlgorithmException, IOException {
		FileDescr newFileDescr = new FileDescr(file,fileDescr.getBlockLength());
		return newFileDescr.getFileMd5().contentEquals(fileDescr.getFileMd5());
	}
	
	/**
	 * Return a boolean array indicating whether a block is available for not.
	 * @return a boolean array of length at least 1 for non-empty files.
	 */
	public boolean[] getBlockAvailability() {
		boolean[] blockAvailable = new boolean[fileDescr.getNumBlocks()];
		for(int blockIndex=0;blockIndex<fileDescr.getNumBlocks();blockIndex++) {
			blockAvailable[blockIndex]=isBlockAvailable(blockIndex);
		}
		return blockAvailable;
	}
	
	/**
	 * 
	 * @return the file descriptor for this file
	 */
	public FileDescr getFileDescr() {
		return fileDescr;
	}
	
	/**
	 * Close the file. The file manager should not be used after calling
	 * this method.
	 * @throws IOException 
	 */
	public void closeFile() throws IOException {
		file.close();
	}
	
}
