package comp90015.idxsrv.peer;

import comp90015.idxsrv.filemgr.BlockUnavailableException;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.textgui.ISharerGUI;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingDeque;

public class Upload extends Thread {

    private LinkedBlockingDeque<Socket> incomingConnections;

    private ISharerGUI tgui;

    public Upload(LinkedBlockingDeque<Socket> incomingConnections,
                  ISharerGUI tgui) {
        this.incomingConnections = incomingConnections;
        this.tgui = tgui;
    }

    @Override
    public void run() {
        tgui.logInfo("Peer upload thread running.");
        try {
            while(!isInterrupted()) {
                Socket socket = incomingConnections.take();
                new ProcessRequest(socket, tgui).start();
            }
        } catch (InterruptedException e) {
            tgui.logWarn("Upload interrupted.");
        } catch (Exception e){
            tgui.logWarn("Upload received unknown exception: " + e.getClass() + e.getMessage());
        }
    }

    private class ProcessRequest extends Thread {

        private ISharerGUI tgui;

        private Socket socket;

        public ProcessRequest(Socket socket, ISharerGUI tgui) {
            this.socket = socket;
            this.tgui = tgui;
        }

        @Override
        public void run() {
            try {

                // Obtain information from socket
                String ip = socket.getInetAddress().getHostAddress();
                int port = socket.getPort();
                tgui.logInfo("Peer upload processing request on connection " + ip + ":" + port);
                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

                boolean transfer = true;
                while (transfer) {

                    // Receives BlockRequest message
                    Message msg;
                    try {
                        msg = readMsg(bufferedReader);
                    } catch (JsonSerializationException e1) {
                        writeMsg(bufferedWriter, new ErrorMsg("Invalid message"));
                        return;
                    }

                    // Evaluate whether the message is BlockRequest or Goodbye
                    if (msg.getClass().getName().equals(BlockRequest.class.getName())) {
                        BlockRequest blockRequestMsg = (BlockRequest) msg;

                        // Obtain information from the Message
                        String filename = blockRequestMsg.filename;
                        String fileMd5 = blockRequestMsg.fileMd5;
                        Integer blockIdx = blockRequestMsg.blockIdx;

                        // Create fileMgr
                        FileMgr fileMgr = null;
                        try {
                            fileMgr = new FileMgr(filename);
                        } catch (IOException e) {
                            tgui.logError("Upload received IO exception at fileMgr");
                            return;
                        } catch (NoSuchAlgorithmException e) {
                            tgui.logError("Upload received NoSuchAlgorithmException at fileMgr");
                            return;
                        }

                        // Obtain bytes of requested block
                        byte[] bytes = new byte[0];
                        if (Objects.equals(fileMd5, fileMgr.getFileDescr().getFileMd5())) {
                            try {
                                if (fileMgr.isBlockAvailable(blockIdx)) {
                                    bytes = fileMgr.readBlock(blockIdx);
                                }
                            } catch (BlockUnavailableException | IOException e) {
                                tgui.logError("Failed to read block for upload");
                                return;
                            }
                        }

                        fileMgr.closeFile();

                        // Encode bytes
                        String encodedBytes = Base64.getEncoder().encodeToString(bytes);

                        // Sends BlockReply message
                        writeMsg(bufferedWriter, new BlockReply(filename, fileMd5, blockIdx, encodedBytes));

                    } else if (msg.getClass().getName().equals(Goodbye.class.getName())) {
                        transfer = false;
                        tgui.logInfo("Upload process ended");
                    } else {
                        writeMsg(bufferedWriter, new ErrorMsg("Expecting BlockRequest or Goodbye only"));
                    }
                }

                bufferedReader.close();
                bufferedWriter.close();
                socket.close();

            } catch (IOException e) {
                tgui.logWarn("Upload process received io exception.");
            } catch (Exception e){
                tgui.logWarn("Upload process received unknown exception: " + e.getClass() + e.getMessage());
            }
        }
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
