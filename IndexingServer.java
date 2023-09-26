import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class IndexingServer {

    private static ConcurrentHashMap<String, ArrayList<String>> fileIndex = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, ArrayList<String>> peerFileLocations = new ConcurrentHashMap<>();
    private static List<String> replicationNodes = Collections.synchronizedList(new ArrayList<>());
    private static final int SERVER_PORT = 10000;
    private static final int PEER_PORT = 20000;
    private static final String REPLICATION_FOLDER = "replica/";

    private static int peerCount = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("Indexing Server Started...");
        int peerId = 1;

        ServerSocket listener = new ServerSocket(SERVER_PORT);
        try {
            while (true) {
                new PeerHandler(listener.accept(), peerId++).start();
            }
        } finally {
            listener.close();
        }
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    private static class PeerHandler extends Thread {
        private Socket socket;
        private int clientId;

        public PeerHandler(Socket socket, int clientId) {
            this.socket = socket;
            this.clientId = clientId;
            System.out.println("\nNew connection with Peer #" + clientId + " at " + socket.getInetAddress());
            peerCount++;
            System.out.println("Total connected peers: " + peerCount);
        }

        public void run() {
            try {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                String clientIp = socket.getInetAddress().getHostAddress();

                Response response = new Response();
                response.setResponseCode(200);
                response.setResponseData("Peer #" + clientId + ".\nDo you want to act as a replication node? (Y/N):");
                out.writeObject(response);

                Request peerRequest = (Request) in.readObject();
                String requestType = peerRequest.getRequestType();
                String replicationChoice = (String) peerRequest.getRequestData();

                if (replicationChoice.equalsIgnoreCase("Y")) {
                    System.out.println("Replication with this node accepted.");

                    if (!replicationNodes.contains(clientIp)) {
                        replicationNodes.add(clientIp);
                    }

                    if (peerFileLocations.containsKey(clientIp)) {
                        peerFileLocations.get(clientIp).add(REPLICATION_FOLDER);
                    } else {
                        ArrayList<String> paths = new ArrayList<>();
                        paths.add(REPLICATION_FOLDER);
                        peerFileLocations.put(clientIp, paths);
                    }

                    response = new Response();
                    response.setResponseCode(200);
                    response.setResponseData(fileIndex);
                    out.writeObject(response);
                }

                response = new Response();
                response.setResponseCode(200);
                response.setResponseData(peerFileLocations.get(clientIp));
                out.writeObject(response);

                while (true) {
                    peerRequest = (Request) in.readObject();
                    requestType = peerRequest.getRequestType();

                    if (requestType.equalsIgnoreCase("REGISTER")) {
                        final ArrayList<String> indexedLocations = register(clientId, clientIp, (ArrayList<String>) peerRequest.getRequestData(), out);
                        response = new Response();
                        response.setResponseCode(200);
                        response.setResponseData(indexedLocations);
                        out.writeObject(response);
                    } else if (requestType.equalsIgnoreCase("LOOKUP")) {
                        System.out.println("\nLooking up a file.");
                        String fileName = (String) peerRequest.getRequestData();
                        System.out.println("Request from Peer #" + clientId + " (" + clientIp + ") to look for file " + fileName);
                        HashMap<Integer, String> searchResults = search(fileName);

                        if (!searchResults.isEmpty()) {
                            response = new Response();
                            response.setResponseCode(200);
                            response.setResponseData(searchResults);
                            out.writeObject(response);
                            System.out.println("File Found.");
                        } else {
                            response = new Response();
                            response.setResponseCode(404);
                            response.setResponseData("File Not Found.");
                            out.writeObject(response);
                            System.out.println("File Not Found.");
                        }
                    } else if (requestType.equalsIgnoreCase("UNREGISTER")) {
                        response = new Response();
                        if (unregister(clientIp)) {
                            response.setResponseCode(200);
                            response.setResponseData("Your files have been unregistered from the indexing server.");
                            System.out.println("Peer #" + clientId + " (" + clientIp + ") has unregistered all its files.");
                        } else {
                            response.setResponseCode(400);
                            response.setResponseData("Error in unregistering files from the indexing server.");
                        }
                        out.writeObject(response);
                    } else if (requestType.equalsIgnoreCase("GET_BACKUP_NODES")) {
                        System.out.println("\n" + clientIp + " requested backup nodes info. Sending backup nodes info.");
                        response = new Response();
                        response.setResponseCode(200);
                        response.setResponseData(replicationNodes);
                        out.writeObject(response);
                        System.out.println("Backup nodes information sent.");
                    } else if (requestType.equalsIgnoreCase("DISCONNECT")) {
                        System.out.println("\nPeer #" + clientId + " disconnecting...");
                        try {
                            socket.close();
                        } catch (IOException e) {
                            System.out.println("Couldn't close the socket.");
                        }
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (EOFException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.out.println("Error handling Peer #" + clientId + ": " + e);
                Thread.currentThread().interrupt();
            }
        }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        public void interrupt() {
            System.out.println("\nConnection with Peer #" + clientId + " closed");
            peerCount--;
            System.out.println("Total connected peers: " + peerCount);
            if (peerCount == 0) {
                System.out.println("No more peers connected.");
            }
        }

        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    

        private ArrayList<String> register(int clientId, String clientIp, ArrayList<String> files, ObjectOutputStream out) throws IOException {
            System.out.println("\nRegistering files from Peer " + clientIp);

            if (peerFileLocations.containsKey(clientIp)) {
                peerFileLocations.get(clientIp).add(files.get(0));
            } else {
                ArrayList<String> paths = new ArrayList<>();
                paths.add(files.get(0));
                peerFileLocations.put(clientIp, paths);
            }
            files.remove(0);

            StringBuffer sb = new StringBuffer();
            sb.append(clientId).append("#").append(clientIp).append("#").append(new SimpleDateFormat("HHmmss").format(Calendar.getInstance().getTime()));
            fileIndex.put(sb.toString(), files);

            System.out.println(files.size() + " files synced with Peer " + clientId + " and added to index database");

            ConcurrentHashMap<String, ArrayList<String>> newFiles = new ConcurrentHashMap<>();
            newFiles.put(sb.toString(), files);
            sendReplicateCommand(newFiles);

            return peerFileLocations.get(clientIp);
        }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        private boolean unregister(String clientIp) throws IOException {
            int oldSize = fileIndex.size();
            ArrayList<String> deleteFiles = null;

            for (Map.Entry<String, ArrayList<String>> entry : fileIndex.entrySet()) {
                String key = entry.getKey();
                ArrayList<String> value = entry.getValue();

                if (key.contains(clientIp)) {
                    deleteFiles = fileIndex.get(key);
                    fileIndex.remove(key);
                }
            }
            int newSize = fileIndex.size();

            if (newSize < oldSize) {
                Request serverRequest = new Request();
                Socket socket = null;
                try {
                    serverRequest.setRequestType("DELETE_DATA");
                    for (String node : replicationNodes) {
                        socket = new Socket(node, PEER_PORT);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        serverRequest.setRequestData(deleteFiles);
                        out.writeObject(serverRequest);
                        out.close();
                        socket.close();
                    }
                    socket = null;
                } catch (Exception e) {
                    System.out.println("Error in replication: " + e);
                } finally {
                    serverRequest = null;
                    if (socket != null && socket.isConnected()) {
                        socket.close();
                    }
                }
            }

            return (newSize < oldSize);
        }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        private HashMap<Integer, String> search(String fileName) {
            HashMap<Integer, String> searchResults = new HashMap<>();
            for (Map.Entry<String, ArrayList<String>> entry : fileIndex.entrySet()) {
                String key = entry.getKey();
                ArrayList<String> value = entry.getValue();

                for (String file : value) {
                    if (file.equalsIgnoreCase(fileName)) {
                        int peerId = Integer.parseInt(key.split("#")[0].trim());
                        String hostAddress = key.split("#")[1].trim();
                        searchResults.put(peerId, hostAddress);
                    }
                }
            }
            return searchResults;
        }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        private void sendReplicateCommand(ConcurrentHashMap<String, ArrayList<String>> newFiles) throws IOException {
            Request serverRequest = new Request();
            Socket socket = null;
            try {
                serverRequest.setRequestType("REPLICATE_DATA");
                for (String node : replicationNodes) {
                    socket = new Socket(node, PEER_PORT);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    serverRequest.setRequestData(newFiles);
                    out.writeObject(serverRequest);
                    out.close();
                    socket.close();
                }
                socket = null;
            } catch (Exception e) {
                System.out.println("Error in replication: " + e);
            } finally {
                serverRequest = null;
                if (socket != null && socket.isConnected()) {
                    socket.close();
                }
            }
        }
    }
}
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++