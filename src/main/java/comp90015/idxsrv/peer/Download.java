package comp90015.idxsrv.peer;

import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.textgui.ISharerGUI;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class Download extends Thread {

    private String fileIp;

    private int filePort;

    private String filename;

    private Queue<Integer> blockIdxQueue;

    private FileMgr fileMgr;

    private ISharerGUI tgui;

    private int timeout;

    public Download(String ip, int port, String filename, int[] blockIdxList, FileMgr fileMgr, ISharerGUI tgui, int timeout){
        this.fileIp = ip;
        this.filePort = port;
        this.filename = filename;
        this.fileMgr = fileMgr;
        this.tgui = tgui;
        this.timeout = timeout;
        blockIdxQueue = new LinkedList<Integer>();
        for(int blockIdx: blockIdxList) blockIdxQueue.add(blockIdx);
    }

    @Override
    public void run() {
        String fileMd5 = fileMgr.getFileDescr().getFileMd5();

        try{
            // Making connection to the fileSharer
            Socket socketPeer = new Socket(fileIp,filePort);
            socketPeer.setSoTimeout(timeout);
            InputStream inputStream = socketPeer.getInputStream();
            OutputStream outputStream = socketPeer.getOutputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));


            // Keep sending requests and reading messages until the file is fully downloaded
            while(!fileMgr.isComplete()){

                // if the blockIdxQueue is empty, select a random unfinished block index and add it to the queue
                if(blockIdxQueue.isEmpty()){
                    try{
                        // Find all the unfinished blocks of the file after the first stage
                        boolean[] blockAvailable = fileMgr.getBlockAvailability();
                        int[] unfinishedBlocks = findLeftBlocks(blockAvailable);

                        // Generate the random index
                        Random r = new Random();
                        int randomIndex = r.nextInt(unfinishedBlocks.length);
                        int blockIdx = unfinishedBlocks[randomIndex];

                        blockIdxQueue.add(blockIdx);
                    } catch (Exception e){
                        tgui.logWarn("Adding elements to the blockIdxQueue received unknown exception: "
                                + e.getClass() + e.getMessage());
                        return;
                    }
                }


                int targetBlockIdx = blockIdxQueue.remove();


                // Send the request
                writeMsg(bufferedWriter,new BlockRequest(filename,fileMd5,targetBlockIdx));

                // Receive the message
                Message msg;
                try{
                    msg = readMsg(bufferedReader);
                }catch (JsonSerializationException e1){
                    writeMsg(bufferedWriter, new ErrorMsg("Invalid message"));
                    return;
                }

                // Check whether the message is blockReply
                if(msg.getClass().getName().equals(BlockReply.class.getName())){
                    BlockReply blockReplyMsg = (BlockReply) msg;
                    int blockIdx = blockReplyMsg.blockIdx;
                    byte[] bytes = Base64.getDecoder().decode(blockReplyMsg.bytes);

                    // Check whether the block is right
                    if(fileMgr.checkBlockHash(blockIdx,bytes)){
                        // Write the block to the file if it doesn't exist yet
                        if(fileMgr.writeBlock(blockIdx,bytes)){
                            tgui.logInfo(filename + " :Block " + blockIdx + " is written to the file.");
                        }
                    } else{
                        tgui.logWarn(filename + ": Block " + blockIdx + " is wrong!");
                        // Add the index of wrong block to the queue again
                        blockIdxQueue.add(blockIdx);
                    }
                } else {
                    writeMsg(bufferedWriter, new ErrorMsg("Expecting BlockRequest only"));
                }

            }


            // Verify the correctness of the whole file
            boolean fileCorrectness = false;
            try{
                fileCorrectness = fileMgr.checkFileHash();
            } catch(NoSuchAlgorithmException e){
                tgui.logError("Correctness verification received NoSuchAlgorithmException at fileMgr");
                return;
            }
            if(fileCorrectness){
                tgui.logInfo(filename + " correctness verification complete.");
            } else{
                tgui.logWarn(filename + " correctness verification failed.");
            }

            fileMgr.closeFile();

            // Sends the Goodbye message
            writeMsg(bufferedWriter,new Goodbye());

            bufferedReader.close();
            bufferedWriter.close();
            socketPeer.close();

        } catch (IOException e){
            tgui.logWarn("Download thread received io exception ");
        } catch (Exception e){
            tgui.logWarn("Download received unknown exception: " + e.getClass() + e.getMessage());
        }
    }

    private int[] findLeftBlocks(boolean[] blockAvailable){
        int numUnfinishedBlocks = 0;
        for(boolean blockStatus : blockAvailable){
            if(!blockStatus){
                numUnfinishedBlocks++;
            }
        }
        int[] unfinishedBlocks = new int[numUnfinishedBlocks];
        int index = 0;
        for(int i=0;i<blockAvailable.length;i++){
            if(!blockAvailable[i]){
                unfinishedBlocks[index] = i;
                index++;
            }
        }
        return unfinishedBlocks;
    }

    private void writeMsg(BufferedWriter bufferedWriter, Message msg) throws IOException {
        bufferedWriter.write(msg.toString());
        bufferedWriter.newLine();
        bufferedWriter.flush();
    }

    private Message readMsg(BufferedReader bufferedReader) throws IOException, JsonSerializationException {
        String jsonStr = bufferedReader.readLine();
        if (jsonStr != null) {
            Message msg = (Message) MessageFactory.deserialize(jsonStr);
            return msg;
        } else {
            throw new IOException();
        }
    }
}
