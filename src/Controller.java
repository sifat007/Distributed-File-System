import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;


public class Controller extends Thread{
	
	ExecutorService threadPool = Executors.newFixedThreadPool(10);

	ArrayList<CSDescriptor> csList = new ArrayList<CSDescriptor>();
	ArrayList<ChunkDescriptor> chunkList = new ArrayList<ChunkDescriptor>();
	
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
        this.startHeartbeatThread();
	}	
	
	private void startHeartbeatThread() {
		//send heartbeats to the chunk server to know if it's alive or not if not replicate files
		// OUTSIDE heartbeat skim
		// this tread is always up
		this.threadPool.execute(new ControllerHeartbeatThread());
	}
	
	public void run() {
		try {
			ServerSocket svsocket = new ServerSocket(this.port);
			while (RUNNING) {
				Socket sock = svsocket.accept();
				this.threadPool.execute(new ControllerSocketThread(sock));
			}
			svsocket.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		this.threadPool.shutdown();
	}
			
}

class CSDescriptor{
	public String host;
	public int port;
	public long free_space;
	public long total_chunks;
}

class ChunkDescriptor{
	public UUID chunkId;
	public String filename;
	public String chunkname;
	public long sequence;
	public long version;
	public long timestamp;
	public ArrayList<CSDescriptor> cslist = new ArrayList<>();
}


class ControllerHeartbeatThread implements Runnable{

	@Override
	public void run() {
		
	}
	
}


class ControllerSocketThread implements Runnable {
	Socket sock;

	public ControllerSocketThread(Socket sock) {
		this.sock = sock;
	}

	public void run() {

		try {
			
			sock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

