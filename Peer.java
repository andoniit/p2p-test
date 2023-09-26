import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



/*CS550 Advanced Operating Systems Programming Assignment 1 Repo
Illinois Institute of Technology

Team Name: KK Students:

Anirudha Kapileshwari (akapileshwari@hawk.iit.edu)
Mugdha Atul Kulkarni (mkulkarni2@hawk.iit.edu) */

public class Peer {
	
	// myIndexedLoc stores the list of all the locations whose files are registered with the Indexing Server.
	private static List<String> myIndexedLoc = Collections.synchronizedList(new ArrayList<String>());
	private static final int PEER_SERVER_PORT = 20000;
	private static final String REPLICATION_PATH = "replica/";
	
	public static void main(String[] args) throws IOException {
		startPeerClient();
		startPeerServer();
	}
	
	private static void startPeerClient() {
		System.out.println("Peer Client Started");
		new PeerClient().start();
	}
	
	private static void startPeerServer() {
		System.out.println("Peer Server Started.....");
		ServerSocket listener = null;
	
		try {
			listener = new ServerSocket(PEER_SERVER_PORT);
			while (true) {
				new PeerServer(listener.accept()).start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (listener != null) {
				try {
					listener.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
  //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  private static class PeerServer extends Thread {
    private Socket socket;
    private LogUtility log = new LogUtility("peer");

    public PeerServer(Socket socket) {
        this.socket = socket;
        log.wlog("File downloading with " + socket.getInetAddress() + " started.");
    }

    @Override
    public void run() {
        OutputStream out = null;
        ObjectInputStream in = null;
        BufferedInputStream fileInput = null;

        try {
            String clientIp = socket.getInetAddress().getHostAddress();
            log.wlog("Serving download request for " + clientIp);

            in = new ObjectInputStream(socket.getInputStream());
            Request request = (Request) in.readObject();

            if (request.getRequestType().equalsIgnoreCase("DOWNLOAD")) {
                handleDownloadRequest(request);
            } else if (request.getRequestType().equalsIgnoreCase("REPLICATE_DATA")) {
                handleReplicateDataRequest(request);
            } else if (request.getRequestType().equalsIgnoreCase("DELETE_DATA")) {
                handleDeleteDataRequest(request);
            }
        } catch (Exception e) {
            log.wlog("Error in serving the request.");
            log.wlog("ERROR: " + e);
        } finally {
            closeStreams(out, in, fileInput, socket);
            Thread.currentThread().interrupt();
        }
    }

    private void handleDownloadRequest(Request request) throws IOException, ClassNotFoundException {
        String fileName = (String) request.getRequestData();
        String fileLocation = FileUtility.getFileLocation(fileName, myIndexedLoc);
        log.wlog("Uploading/Sending file " + fileName);

        File file = new File(fileLocation + fileName);
        byte[] mybytearray = new byte[(int) file.length()];
        try (BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(file));
             OutputStream out = socket.getOutputStream()) {
            fileInput.read(mybytearray, 0, mybytearray.length);
            out.write(mybytearray, 0, mybytearray.length);
            out.flush();
        }
        log.wlog("File sent successfully.");
    }

    private void handleReplicateDataRequest(Request request) {
        final ConcurrentHashMap<String, ArrayList<String>> data = (ConcurrentHashMap<String, ArrayList<String>>) request.getRequestData();
        new ReplicationService(data).start();
    }

    private void handleDeleteDataRequest(Request request) {
        final ArrayList<String> deleteFiles = (ArrayList<String>) request.getRequestData();
        if (deleteFiles != null) {
            for (String fileName : deleteFiles) {
                File file = new File(REPLICATION_PATH + fileName);
                file.delete();
                file = null;
            }
        }
    }

    private void closeStreams(OutputStream out, ObjectInputStream in, BufferedInputStream fileInput, Socket socket) {
        try {
            if (out != null)
                out.close();

            if (in != null)
                in.close();

            if (fileInput != null)
                fileInput.close();

            if (socket != null)
                socket.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    @Override
    public void interrupt() {
        log.closeLogger();
        super.interrupt();
    }
}

	    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		private static class PeerClient extends Thread {
			// Constants for menu options
			private static final int OPTION_REGISTER = 1;
			private static final int OPTION_LOOKUP = 2;
			private static final int OPTION_UNREGISTER = 3;
			private static final int OPTION_PRINT_LOG = 4;
			private static final int OPTION_EXIT = 5;
		
			public void run() {
				Socket socket = null;
				ObjectInputStream in = null;
				BufferedReader input = null;
				ObjectOutputStream out = null;
				Request peerRequest = null;
				Response serverResponse = null;
		
				try {
					input = new BufferedReader(new InputStreamReader(System.in));
					System.out.println("Enter Server IP Address:");
					String serverAddress = input.readLine();
		
					if (serverAddress.trim().length() == 0 || !IPAddressValidator.validate(serverAddress)) {
						System.out.println("Invalid Server IP Address.");
						System.exit(0);
					}
		
					// Make a connection with the server using the specified host address and port 10000
					socket = new Socket(serverAddress, 10000);
		
					// Initializing output stream using the socket's output stream
					out = new ObjectOutputStream(socket.getOutputStream());
					out.flush();
		
					// Initializing input stream using the socket's input stream
					in = new ObjectInputStream(socket.getInputStream());
		
					// Read the initial welcome message from the server
					serverResponse = (Response) in.readObject();
					System.out.print((String) serverResponse.getResponseData());
					String replicaChoice = input.readLine();
		
					// Setup a Request object with Request Type = REPLICATION and Request Data = Choice
					peerRequest = new Request();
					peerRequest.setRequestType("REPLICATION");
					peerRequest.setRequestData(replicaChoice);
					out.writeObject(peerRequest);
		
					if (replicaChoice.equalsIgnoreCase("Y")) {
						// Read the Replication response from the server
						myIndexedLoc.add(REPLICATION_PATH);
						serverResponse = (Response) in.readObject();
						ConcurrentHashMap<String, ArrayList<String>> data = (ConcurrentHashMap<String, ArrayList<String>>) serverResponse.getResponseData();
						new ReplicationService(data).start();
					}
		
					// Previously indexed locations if any
					serverResponse = (Response) in.readObject();
					ArrayList<String> indexedLocations = (ArrayList<String>) serverResponse.getResponseData();
					if (indexedLocations != null) {
						for (String x : indexedLocations) {
							if (!myIndexedLoc.contains(x)) {
								myIndexedLoc.add(x);
							}
						}
					}
		
					while (true) {
						// Display different choices to the user
						System.out.println("\nWhat do you want to do?");
						System.out.println("1. Register files with indexing server.");
						System.out.println("2. Lookup for a file at index server.");
						System.out.println("3. Unregister all files of this peer from the indexing server.");
						System.out.println("4. Print download log of this peer.");
						System.out.println("5. Exit.");
						System.out.print("Enter choice and press ENTER:");
		
						int option;
						try {
							option = Integer.parseInt(input.readLine());
						} catch (NumberFormatException e) {
							System.out.println("Wrong choice. Try again!!!");
							continue;
						}
		
						switch (option) {
							case OPTION_REGISTER:
								handleRegisterFiles(input, out, in);
								break;
		
							case OPTION_LOOKUP:
								handleLookupFile(input, out, in);
								break;
		
							case OPTION_UNREGISTER:
								handleUnregisterFiles(input, out, in);
								break;
		
							case OPTION_PRINT_LOG:
								new LogUtility("peer").printLogs();
								break;
		
							case OPTION_EXIT:
								handleExit(socket, out);
								break;
		
							default:
								System.out.println("Wrong choice. Try again!!!");
								break;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					closeStreams(socket, out, in, input);
				}
			}
			//===================================================================================================================

			private void handleRegisterFiles(BufferedReader input, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
				System.out.println("\nEnter path of the files to sync with indexing server:");
				String path = input.readLine();
		
				if (path.trim().length() == 0) {
					System.out.println("Invalid Path.");
					return;
				}
		
				// Retrieve all the files from the user's specified location
				ArrayList<String> files = FileUtility.getFiles(path);
		
				// Add the user's entered file/path to peer's indexed location's list
				File file = new File(path);
				if (file.isFile()) {
					myIndexedLoc.add(path.substring(0, path.lastIndexOf("/")));
					System.out.println(path.substring(0, path.lastIndexOf("/")));
					files.add(0, path.substring(0, path.lastIndexOf("/")));
				} else if (file.isDirectory()) {
					myIndexedLoc.add(path);
					files.add(0, path);
				}
		
				// 1 because path is always there
				if (files.size() > 1) {
					long startTime = System.currentTimeMillis();
		
					// Setup a Request object with Request Type = REGISTER and Request Data = files array list
					Request peerRequest = new Request();
					peerRequest.setRequestType("REGISTER");
					peerRequest.setRequestData(files);
					out.writeObject(peerRequest);
		
					// Retrieve response from the server
					Response serverResponse = (Response) in.readObject();
					long endTime = System.currentTimeMillis();
					double time = (double) Math.round(endTime - startTime) / 1000;
		
					// If Response is success i.e. Response Code = 200, then print success message else error message
					if (serverResponse.getResponseCode() == 200) {
						System.out.println((files.size() - 1) + " files registered with indexing server. Time taken:" + time + " seconds.");
					} else {
						System.out.println("Unable to register files with the server. Please try again later.");
					}
				} else {
					System.out.println("0 files found at this location. Nothing registered with indexing server.");
				}
			}
			//===================================================================================================================
	
			private void handleLookupFile(BufferedReader input, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
				System.out.println("\nEnter name of the file you want to look for at the indexing server:");
				String fileName = input.readLine();
		
				long startTime = System.currentTimeMillis();
				// Setup a Request object with Request Type = LOOKUP and Request Data = file to be searched
				Request peerRequest = new Request();
				peerRequest.setRequestType("LOOKUP");
				peerRequest.setRequestData(fileName);
				out.writeObject(peerRequest);
		
				Response serverResponse = (Response) in.readObject();
				long endTime = System.currentTimeMillis();
				double time = (double) Math.round(endTime - startTime) / 1000;
		
				// If Response is success i.e. Response Code = 200, then perform download operation else error message
				if (serverResponse.getResponseCode() == 200) {
					System.out.println("File Found. Lookup time: " + time + " seconds.");
		
					// Response Data contains the List of Peers which contain the searched file
					HashMap<Integer, String> lookupResults = (HashMap<Integer, String>) serverResponse.getResponseData();
		
					// Printing all Peer details that contain the searched file
					if (lookupResults != null) {
						for (Map.Entry<Integer, String> e : lookupResults.entrySet()) {
							System.out.println("\nPeer ID:" + e.getKey().toString());
							System.out.println("Host Address:" + e.getValue().toString());
						}
					}
		
					// If the file is a Text file, then we can print or else only download the file
					if (fileName.trim().endsWith(".txt")) {
						System.out.print("\nDo you want to download (D) or print this file (P)? Enter (D/P):");
						String download = input.readLine();
		
						String hostAddress;
						// In case there are more than 1 peer, then the user will select which peer to use for download
						if (lookupResults.size() > 1) {
							System.out.print("Enter Peer ID from which you want to download the file:");
							int peerId = Integer.parseInt(input.readLine());
							hostAddress = lookupResults.get(peerId);
						} else {
							Map.Entry<Integer, String> entry = lookupResults.entrySet().iterator().next();
							hostAddress = entry.getValue();
						}
		
						if (download.equalsIgnoreCase("D")) {
							System.out.println("The file will be downloaded in the 'downloads' folder in the current location.");
							// Obtain the searched file from the specified Peer
							obtain(hostAddress, 20000, fileName, out, in);
						} else if (download.equalsIgnoreCase("P")) {
							// Obtain the searched file from the specified Peer and print its contents
							obtain(hostAddress, 20000, fileName, out, in);
							FileUtility.printFile(fileName);
						}
					} else {
						System.out.print("\nDo you want to download this file?(Y/N):");
						String download = input.readLine();
						if (download.equalsIgnoreCase("Y")) {
							String hostAddress;
							// In case there are more than 1 peer, then the user will select which peer to use for download
							if (lookupResults.size() > 1) {
								System.out.print("Enter Peer ID from which you want to download the file:");
								int peerId = Integer.parseInt(input.readLine());
								hostAddress = lookupResults.get(peerId);
							} else {
								Map.Entry<Integer, String> entry = lookupResults.entrySet().iterator().next();
								hostAddress = entry.getValue();
							}
							// Obtain the searched file from the specified Peer
							obtain(hostAddress, 20000, fileName, out, in);
						}
					}
				} else {
					System.out.println((String) serverResponse.getResponseData());
					System.out.println("Lookup time: " + time + " seconds.");
				}
			}
			//===================================================================================================================

			private void obtain(String hostAddress, int i, String fileName, ObjectOutputStream out,
					ObjectInputStream in) {
			}
			//===================================================================================================================

			private void handleUnregisterFiles(BufferedReader input, ObjectOutputStream out, ObjectInputStream in) throws IOException, ClassNotFoundException {
				// Confirming user's un-register request
				System.out.print("\nAre you sure (Y/N)?:");
				String confirm = input.readLine();
		
				if (confirm.equalsIgnoreCase("Y")) {
					long startTime = System.currentTimeMillis();
					// Setup a Request object with Request Type = UNREGISTER and Request Data = general message
					Request peerRequest = new Request();
					peerRequest.setRequestType("UNREGISTER");
					peerRequest.setRequestData("Un-register all files from the index server.");
					out.writeObject(peerRequest);
					long endTime = System.currentTimeMillis();
					double time = (double) Math.round(endTime - startTime) / 1000;
		
					Response serverResponse = (Response) in.readObject();
					System.out.println((String) serverResponse.getResponseData());
					System.out.println("Time taken:" + time + " seconds.");
				}
			}
		//===================================================================================================================

			private void handleExit(Socket socket, ObjectOutputStream out) throws IOException {
				// Setup a Request object with Request Type = DISCONNECT and Request Data = general message
				Request peerRequest = new Request();
				peerRequest.setRequestType("DISCONNECT");
				peerRequest.setRequestData("Disconnecting from the server.");
				out.writeObject(peerRequest);
				System.out.println("Thanks for using this system.");
				System.exit(0);
			}
			//===================================================================================================================
			private void closeStreams(Socket socket, ObjectOutputStream out, ObjectInputStream in, BufferedReader input) {
				try {
					// Closing all streams. Close the stream only if it is initialized
					if (out != null)
						out.close();
		
					if (in != null)
						in.close();
		
					if (socket != null)
						socket.close();
		
					if (input != null)
						input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	
	
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	
private static class ReplicationService extends Thread {
    private ConcurrentHashMap<String, ArrayList<String>> data;

    public ReplicationService(ConcurrentHashMap<String, ArrayList<String>> data) {
        this.data = data;
    }

    @Override
    public void run() {
        for (Map.Entry<String, ArrayList<String>> entry : data.entrySet()) {
            String peerKey = entry.getKey();
            ArrayList<String> filesToReplicate = entry.getValue();

            // Extract host address from the peerKey
            String hostAddress = peerKey.split("#")[1].trim();

            for (String file : filesToReplicate) {
                // Replicate file from the respective peer
                replicate(hostAddress, 20000, file);
            }
        }
        // Thread is done, no need to explicitly interrupt it.
    }

    private void replicate(String hostAddress, int port, String fileName) {
        // Use the FileUtility.replicateFile method to replicate the file
        FileUtility.replicateFile(hostAddress, port, fileName);
    }
}

	    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

}
