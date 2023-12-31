import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;


/*CS550 Advanced Operating Systems Programming Assignment 1 Repo
Illinois Institute of Technology

Team Name: KK Students:

Anirudha Kapileshwari (akapileshwari@hawk.iit.edu)
Mugdha Atul Kulkarni (mkulkarni2@hawk.iit.edu) */

/*
 * This class handles all file related operations 
 * like get file loaction, file downlaod, replicate file
 * 
 */

public class FileManager {

	private static final String downloadLocation = "downloads/";
	private static final String replicaLocation = "replica/";
	private static final int BUFFER_SIZE = 1024 * 64; // 64 KiloBytes
	


	public static ArrayList<String> getFiles(String path) {
		ArrayList<String> files = new ArrayList<String>();
		File folder = new File(path);
		if (folder.isDirectory()) {
			File[] listOfFiles = folder.listFiles();
			//path = path.endsWith("/") ? path : path.concat("/");
			
			if (listOfFiles != null) {
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].isFile()) {
						String file = listOfFiles[i].getName();
						if(!file.endsWith("~")) {
							files.add(file);
						}	
					}
				}
			}
		} else if (folder.isFile()) {
			files.add(path.substring(path.lastIndexOf("/") + 1, path.length()));
		}
		
		return files;
	}
	
	
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



	public static String getFileLocation(String fileName, List<String> locations) {
		String fileLocation = "";
		boolean fileFound = false;
		
		for (String path : locations) {
			File folder = new File(path);
			File[] listOfFiles = folder.listFiles();
			
			for (int i = 0; i < listOfFiles.length; i++) {
				if(listOfFiles[i].getName().equals(fileName)) {
					fileLocation = path.endsWith("/") ? path : path.concat("/");
					fileFound = true;
					break;
				}
			}
		}
		fileLocation = fileFound ? fileLocation : "File Not Found."; 
		return fileLocation;
	}
	
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


	
	public static boolean downloadFile(String hostAddress, int port, String fileName) {
		InputStream in = null;
		BufferedOutputStream fileOutput = null;
		ObjectOutputStream out = null;
		Socket socket = null;
		boolean isDownloaded = false;
		
		try {
			// Establish connection to the peer which contains the file for downloading.
			socket = new Socket(hostAddress, port);
			System.out.println("\nDownloading file " + fileName);
			
			// Create a download folder if it doesn't exist
			File file = new File(downloadLocation);
			if (!file.exists())
				file.mkdir();

			// Create an output stream using the socket's output stream.
			out = new ObjectOutputStream(socket.getOutputStream());
			out.flush();

			System.out.println("Requesting file.........");
			// Setup a Request object with Request Type = DOWNLOAD and Request Data = name of the file to be downloaded
			Request request = new Request();
			request.setRequestType("DOWNLOAD");
			request.setRequestData(fileName);
			out.writeObject(request);

			// Download file from the output stream
			System.out.println("Downloading file........");
			byte[] mybytearray = new byte[BUFFER_SIZE];
			in = socket.getInputStream();
			fileOutput = new BufferedOutputStream(new FileOutputStream(downloadLocation + fileName));
			
			// Reading incoming stream in chunks
			int bytesRead;
			while ((bytesRead = in.read(mybytearray, 0, mybytearray.length)) > 0)
			{
				fileOutput.write(mybytearray, 0, bytesRead);
			}
			
			if((new File(downloadLocation + fileName)).length() == 0) {
				isDownloaded = false;
				(new File(downloadLocation + fileName)).delete();
			} else {
				isDownloaded = true;
			}
		} catch(SocketException e) {
			//System.out.println("Unable to connect to the host. Unable to  download file. Try using a different peer if available.");
			isDownloaded = false;
			
		} catch (Exception e) {
			isDownloaded = false;
			
		} finally {
			try {
				// Closing all streams. Close the stream only if it is initialized
				if (out != null)
					out.close();
				
				if (in != null)
					in.close();
				
				if (fileOutput != null)
					fileOutput.close();
				
				if (socket != null)
					socket.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return isDownloaded;
	}
	
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++





	public static boolean replicateFile(String hostAddress, int port, String fileName) {
		InputStream in = null;
		BufferedOutputStream fileOutput = null;
		ObjectOutputStream out = null;
		Socket socket = null;
		boolean isReplicated = false;
		LogMaker log = new LogMaker("replication");
		
		try {
			long startTime = System.currentTimeMillis();
			
			// Establish connection to the peer which contains the file for downloading.
			socket = new Socket(hostAddress, port);
			
			// Create a replica folder if it doesn't exist
			File file = new File(replicaLocation);
			if (!file.exists())
				file.mkdir();

			// Create an output stream using the socket's output stream.
			out = new ObjectOutputStream(socket.getOutputStream());
			out.flush();

			log.wlog("Requesting file ... " + fileName);
			// Setup a Request object with Request Type = DOWNLOAD and Request Data = name of the file to be downloaded
			Request request = new Request();
			request.setRequestType("DOWNLOAD");
			request.setRequestData(fileName);
			out.writeObject(request);

			// Download file from the output stream
			log.wlog("Downloading file ... " + fileName);
			byte[] mybytearray = new byte[BUFFER_SIZE];
			in = socket.getInputStream();
			fileOutput = new BufferedOutputStream(new FileOutputStream(replicaLocation + fileName));
			
			int bytesRead;
			while ((bytesRead = in.read(mybytearray, 0, mybytearray.length)) > 0)
			{
				fileOutput.write(mybytearray, 0, bytesRead);
			}			
			
			long endTime = System.currentTimeMillis();
			double time = (double) Math.round(endTime - startTime) / 1000;
			log.wlog("File downloaded successfully in " + time + " seconds.");
			isReplicated = true;
		} catch(SocketException e) {
			log.wlog("Unable to connect to the host. Unable to  download file.");
			isReplicated = false;
			log.wlog("Error:" + e);
		} catch (Exception e) {
			log.wlog("Unable to download file. Please check if you have write permission.");
			isReplicated = false;
			log.wlog("Error:" + e);
		} finally {
			try {
				// Closing all streams. Close the stream only if it is initialized
				if (out != null)
					out.close();
				
				if (in != null)
					in.close();
				
				if (fileOutput != null)
					fileOutput.close();
				
				if (socket != null)
					socket.close();
				
				if (log != null)
					log.closeLogger();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return isReplicated;
	}

	
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	
}