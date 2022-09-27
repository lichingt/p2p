package comp90015.idxsrv.filemgr;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import comp90015.idxsrv.message.JsonElement;
import comp90015.idxsrv.message.JsonSerializable;

/**
 * A descriptor of a file, containing MD5 hash information for
 * all blocks of the file.
 * @author aaron
 *
 */
@JsonSerializable
public class FileDescr {
	
	@JsonElement
	private Long fileLength;
	
	@JsonElement
	private Integer blockLength;
	
	@JsonElement
	private Integer numBlocks;
	
	@JsonElement
	private String fileMd5;
	
	@JsonElement
	private String[] blockMd5;
	
	/**
	 * A blank object to support json serialization.
	 */
	public FileDescr() {
		
	}
	
	/**
	 * Create a file descriptor for a given file. The default block length is 16MB.
	 * If the file is empty, all MD5 hashes will be the empty string.
	 * @param file the file to create the file descriptor for
	 * @throws IOException if the file cannot be accessed
	 * @throws NoSuchAlgorithmException if the MD5 algorithm is unavailable
	 */
	public FileDescr(RandomAccessFile file) throws IOException, NoSuchAlgorithmException {
		fileLength = file.length();
		blockLength=16*1024*1024;
		numBlocks = (int)(fileLength/blockLength + (fileLength%blockLength>0?1:0));
		if(fileLength==0) return;
		init(file);
	}
	
	/**
	 * Create a file descriptor for a given file, using a given block length.
	 * If the file is empty, all MD5 hashes will be the empty string.
	 * @param file the file to create the file descriptor for
	 * @param blockLength the block length to use
	 * @throws IOException if the file cannot be accessed
	 * @throws NoSuchAlgorithmException if the MD5 algorithm is unavailable
	 */
	public FileDescr(RandomAccessFile file, int blockLength) throws NoSuchAlgorithmException, IOException {
		fileLength = file.length();
		this.blockLength=blockLength;
		numBlocks = (int)(fileLength/blockLength + (fileLength%blockLength>0?1:0));
		if(fileLength==0) return;
		init(file);
	}
	
	private void init(RandomAccessFile file) throws IOException, NoSuchAlgorithmException {
		blockMd5 = new String[numBlocks];
		MessageDigest fileDigest = MessageDigest.getInstance("MD5");
		MessageDigest blockDigest = MessageDigest.getInstance("MD5");
		for(int b = 0; b < numBlocks; b+=1) {
			int numBytes = getNumBlockBytes(b);
			long blockOffset = getBlockOffset(b);
			byte[] blockBytes = new byte[(int)numBytes];
			file.seek(blockOffset);
			file.readFully(blockBytes,0,numBytes);
			fileDigest.update(blockBytes);
			blockDigest.update(blockBytes);
			blockMd5[b]=bytesToHex(blockDigest.digest());
			blockDigest.reset();
		}
		fileMd5=bytesToHex(fileDigest.digest());
	}
	
	/**
	 * 
	 * @return the block length in bytes
	 */
	public int getBlockLength() {
		return blockLength;
	}
	
	/**
	 * 
	 * @return the file's length in bytes
	 */
	public long getFileLength() {
		return fileLength;
	}
	
	/**
	 * 
	 * @return the number of blocks
	 */
	public int getNumBlocks() {
		return numBlocks;
	}
	
	/**
	 * Compute the number of bytes at a given block index, which is equal to
	 * the block length for all blocks but the last one, which may be less than
	 * the block length.
	 * @param blockIdx the index of the block, which must be less than the number of blocks
	 * @return the number of bytes in the block
	 */
	public int getNumBlockBytes(int blockIdx) {
		if(blockIdx<numBlocks) {
			return (int) Math.min(blockLength,fileLength - ((long)blockIdx)*blockLength);
		} else {
			throw new InvalidBlockIndexException();
		}
		
	}
	
	/**
	 * Compute the offset of a given block given it's index
	 * @param blockIdx the index of the block, which must be less than the number of blocks
	 * @return the block's offset
	 */
	public long getBlockOffset(int blockIdx) {
		if(blockIdx<numBlocks) return ((long)blockIdx)*blockLength;
		throw new InvalidBlockIndexException();
	}
	
	/**
	 * 
	 * @return the MD5 hash of the file's content, or the empty string if the file has length 0
	 */
	public String getFileMd5() {
		if(fileLength>0) {
			return fileMd5;
		} else {
			return "";
		}
	}
	
	/**
	 * 
	 * @param blockIdx
	 * @return the MD5 hash of the block at the given blockIdx, or the empty string if the file has length 0
	 */
	public String getBlockMd5(int blockIdx) {
		if(fileLength>0) {
			return blockMd5[blockIdx];
		} else {
			return "";
		}
	}
	
	// https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for (int j = 0; j < bytes.length; j++) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = HEX_ARRAY[v >>> 4];
	        hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
	    }
	    return new String(hexChars);
	}

}
