import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import sun.security.x509.FreshestCRLExtension;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;


public class Controller extends Thread{
	
	ExecutorService threadPool = Executors.newFixedThreadPool(10);

	HashMap<String, Set<String>> filenameToChunkMap = new HashMap<>();
	HashMap<String,CSDescriptor> chunkServerMap = new HashMap<String,CSDescriptor>(); //mapping from host:port to chunk server descriptor
	HashMap<String,ChunkDescriptor> chunkMap = new HashMap<String,ChunkDescriptor>(); //mapping from chuck name to chunk descriptor
	
	boolean RUNNING = true;
	
	int port;
	
	public static void main(String[] args) throws Exception {	
		new File(Utils.STORAGE_PATH).mkdirs();
		new Controller();
	}
	
	public Controller() {
		// System.out.println("server");
		File f = new File("controller_node.txt");
        Scanner scanner =null;
		try {
			scanner = new Scanner(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
        if(scanner.hasNext()){
        	String[] str = scanner.nextLine().split(":");
        	this.port = Integer.parseInt(str[1]);
        }
        this.start();
        new ControllerHeartbeatThread().start();
	}	
	
	
	public void run() {
		try {			
			ServerSocket svsocket = new ServerSocket(this.port);
			System.out.println("Server started...");
			while (RUNNING) {
				Socket sock = svsocket.accept();
				this.threadPool.execute(new ControllerSocketThread(sock,this));
			}
			svsocket.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		this.threadPool.shutdown();
	}
			
}

class CSDescriptor implements Comparable<CSDescriptor>{
	//public String host;
	//public int port;
	public long free_space;
	public long total_chunks;
	public CSDescriptor(long free_space, long total_chunks) {
		this.free_space = free_space;
		this.total_chunks = total_chunks;
	}
	@Override
	public int compareTo(CSDescriptor o) {
		return (int)(this.total_chunks - o.total_chunks);
	}
	
	@Override
	public String toString() {
		return "" + this.free_space + "," + this.total_chunks;
	}
	
	
}

class ChunkDescriptor{
	public String originalFilename;
	//public String chunkname;
	public long sequence;
	public HashSet<String> cslist = new HashSet<>();
	
	public ChunkDescriptor(String originalFilename, long sequence, String chunkServerId) {
		this.originalFilename = originalFilename;
		this.sequence = sequence;
		cslist.add(chunkServerId);
	}
	
	@Override
	public String toString() {
		return originalFilename + "," + sequence + "," + cslist;
	}
}


class ControllerHeartbeatThread extends Thread{

	@Override
	public void run() {
		
	}
	
}


class ControllerSocketThread implements Runnable {
	Socket sock;
	Controller controller;

	public ControllerSocketThread(Socket sock, Controller controller) {
		this.sock = sock;
		this.controller = controller;
	}
	
	

	public void run() {

		try {
			String action = Utils.readStringFromSocket(sock);
			System.out.println(action);
			if(action.equals("#MAJOR_HEARTBEAT#")) {
				//read chunk server info
				String server_info = Utils.readStringFromSocket(sock);
				String[] info = server_info.split(",");
				String server_id = info[0] + ":" + info[1];
				long free_space = Long.parseLong(info[2]);
				long total_chunks = Long.parseLong(info[3]);
				controller.chunkServerMap.put(server_id, new CSDescriptor(free_space, total_chunks));
				System.out.println(server_info);
				for(int i = 0 ; i < total_chunks; i++) {
					readChunckMetadata(server_id);
				}
				//System.out.println(controller.chunkMap);
				
			}else if(action.equals("#MINOR_HEARTBEAT#")) {
				boolean request_major_heartbeat = false;
				//read chunk server info
				String server_info = Utils.readStringFromSocket(sock);
				String[] info = server_info.split(",");
				String server_id = info[0] + ":" + info[1];
				long free_space = Long.parseLong(info[2]);
				long total_chunks = Long.parseLong(info[3]);
				if(controller.chunkServerMap.containsKey(server_id)==false)request_major_heartbeat = true;
				controller.chunkServerMap.put(server_id, new CSDescriptor(free_space, total_chunks));
				long new_chunk_count = Long.parseLong(Utils.readStringFromSocket(sock));
				//System.out.println("Receiving " +new_chunk_count+ " new metadata");
				for(int i = 0 ; i < new_chunk_count; i++) {
					readChunckMetadata(server_id);
				}
				Utils.writeStringToSocket(sock, request_major_heartbeat + "");
				//System.out.println(controller.chunkMap);				
			}else if(action.equals("#CHUNKLIST#")) {
				String filename = Utils.readStringFromSocket(sock);
				if(controller.filenameToChunkMap.containsKey(filename)==false) {
					Utils.writeStringToSocket(sock, "$FILE_NOT_FOUND$");
				}else {
					Utils.writeStringToSocket(sock, "$FILE_FOUND$");
					Set<String> files = controller.filenameToChunkMap.get(filename);
					String str = "";
					for(String chunkName: files) {
						str += chunkName + ",";
					}
					str = str.substring(0, str.length()-1);
					Utils.writeStringToSocket(sock, str);
				}
								
			}else if(action.equals("#GET_SERVERS_FOR_A_CHUNK#")) {
				String chunkName = Utils.readStringFromSocket(sock);
				ChunkDescriptor chunkDescriptor = controller.chunkMap.get(chunkName);
				String serverList = "";
				for(String s:chunkDescriptor.cslist) {
					serverList += "," + s;
				}
				serverList = serverList.substring(1);
				Utils.writeStringToSocket(sock, serverList);
			}else if(action.equals("#REQUEST_FREE_CHUNK_SERVER#")) {
				//TODO: sorted hashmap
				Map<String, CSDescriptor> sortedMap = Utils.sortByValue(controller.chunkServerMap);
				String str = "";
				for(Entry<String, CSDescriptor> e:sortedMap.entrySet()) {
					System.out.println(e.getKey() + ":" + e.getValue());
					str += e.getKey() + ",";
				}
				Utils.writeStringToSocket(sock, str);
			}
			sock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	private void readChunckMetadata(String server_id) throws IOException {
		String chunk_info = Utils.readStringFromSocket(sock);
		System.out.println(chunk_info);
		String[] info_arr = chunk_info.split(",");
		String chunkName = info_arr[0];
		String originalFileName = info_arr[1];
		long sequence = Long.parseLong(info_arr[2]);
		controller.chunkMap.put(chunkName, new ChunkDescriptor(originalFileName,sequence,server_id));	
		Set<String> set = controller.filenameToChunkMap.get(originalFileName);
		if(set == null) {
			set = new HashSet<>();
		}
		set.add(chunkName);
		controller.filenameToChunkMap.put(originalFileName, set);
	}
}

