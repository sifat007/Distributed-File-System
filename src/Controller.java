import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import sun.security.x509.FreshestCRLExtension;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;


public class Controller extends Thread{
	
	ExecutorService threadPool = Executors.newFixedThreadPool(10);

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

class CSDescriptor{
	//public String host;
	//public int port;
	public long free_space;
	public long total_chunks;
	public CSDescriptor(long free_space, long total_chunks) {
		this.free_space = free_space;
		this.total_chunks = total_chunks;
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
				
				for(int i = 0 ; i < total_chunks; i++) {
					String chunk_info = Utils.readStringFromSocket(sock);
					System.out.println(chunk_info);
					String[] info_arr = chunk_info.split(",");
					String chunkName = info_arr[0];
					String originalFileName = info_arr[1];
					long sequence = Long.parseLong(info_arr[2]);
					controller.chunkMap.put(chunkName, new ChunkDescriptor(originalFileName,sequence,server_id));					
				}
				System.out.println(controller.chunkMap);
				
			}else if(action.equals("#MINOR_HEARTBEAT#")) {
				
			}
			sock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

