import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;
import java.net.*;
import java.security.*;

public class ChunkServer extends Thread{
	
	ExecutorService threadPool = Executors.newFixedThreadPool(20);
	
	HashMap<String, ArrayList<File>> fileToChunkHash; //hashmap: filename -> list of chunk files
	HashMap<String, ArrayList<File>> transientFileToChunkHash; //hashmap: filename -> list of chunk files
	
	HashMap<String, File> chunkToMetaDataHash; //hashmap: chunk fileanme -> file handle for metadata
	HashMap<String, File> chunkToChecksumHash; //hashmap: chunk fileanme -> file handle for checksum
	
	
	boolean RUNNING = true;
	
	int port;
	String CONTROLLER_HOST;
	int CONTROLLER_PORT;
	
	public static void main(String[] args) {
		new File(Utils.STORAGE_PATH).mkdirs();
		int port = 0;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			System.out.println("Usage: java ChunkServer <port>");
			return;
		}
		new ChunkServer(port);
	}
	
	public ChunkServer(int port) {
		this.port = port;
		this.fileToChunkHash = new HashMap<>();
		this.transientFileToChunkHash = new HashMap<>();
		this.chunkToChecksumHash = new HashMap<>();
		this.chunkToMetaDataHash = new HashMap<>();
		
		File f = new File("controller_node.txt");
        Scanner scanner =null;
		try {
			scanner = new Scanner(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
        if(scanner.hasNext()){
        	String[] str = scanner.nextLine().split(":");
        	this.CONTROLLER_HOST = str[0];
        	this.CONTROLLER_PORT = Integer.parseInt(str[1]);
        }
        this.start();
        new IOThread(this).start();
        new ChunkServerHeartbeatThread(this).start();
        
	}
	
	
	@Override
	public void run() {
		try {
			ServerSocket svsocket = new ServerSocket(this.port);
			System.out.println("Starting server...");
			while (RUNNING) {
				Socket sock = svsocket.accept();
				this.threadPool.execute(new ChunkServerSocketThread(sock,this));
			}
			svsocket.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		this.threadPool.shutdown();
	}
	
	public void saveChunkAndForward(File chunkFile, String originalFileName, int sequence, String forwardChunkServers) throws IOException{
		ArrayList<File> chunks = fileToChunkHash.get(originalFileName);
		if(chunks == null) {
			chunks = new ArrayList<>();			
		}
		chunks.add(chunkFile);
		fileToChunkHash.put(originalFileName, chunks);
		
		//track new files; will be removed as soon as pushed to the controller
		chunks = transientFileToChunkHash.get(originalFileName);
		if(chunks == null) {
			chunks = new ArrayList<>();			
		}
		chunks.add(chunkFile);
		transientFileToChunkHash.put(originalFileName, chunks);
		
		File checksumFile = this.createChecksumFile(chunkFile);
		chunkToChecksumHash.put(chunkFile.getName(),checksumFile);
		
		int version = 0;
		File metaFile = new File(chunkFile.getPath() + ".metadata");
		if(metaFile.exists()) {
			try(Scanner scanner = new Scanner(metaFile)){
				version = scanner.nextInt();
			}		
		}
		
		
		try(PrintWriter out = new PrintWriter( metaFile ) ){
		    out.println( version + 1 ); //increment version
		    out.println(sequence);
		    out.println(originalFileName);
		    out.println(System.currentTimeMillis());
		}
		
		chunkToMetaDataHash.put(chunkFile.getName(),metaFile);
				
		//TODO: forward the chunk to the next chunk server
		String[] serverList = forwardChunkServers.split(",");
		if(serverList.length > 0 && serverList[0].length() > 0) {
			String[] nextServer = serverList[0].split(":");
			forwardChunkServers = "";
			if(serverList.length > 1) {
				forwardChunkServers = serverList[1];
				for(int i = 2; i < serverList.length; i++) {
					forwardChunkServers += ","+serverList[i];
				}
			}
			final String _forwardChunkServers = forwardChunkServers;
			
			this.threadPool.execute(new Runnable() {			
				@Override
				public void run() {
					Socket sock;
					try {
						sock = new Socket(nextServer[0], Integer.parseInt(nextServer[1]));
						Utils.writeStringToSocket(sock, "#SAVE_CHUNK#");
						Utils.writeStringToSocket(sock, _forwardChunkServers); //send ArrayList of other chunks to forward the file to
						Utils.writeStringToSocket(sock, originalFileName);
						Utils.writeStringToSocket(sock, sequence +"");
						Utils.writeFileToSocket(sock, chunkFile);
						sock.close();
					} catch (NumberFormatException | IOException e) {
						e.printStackTrace();
					} //	
					
				}
			});
		}
		
		
		
	}
	
	private File createChecksumFile(File originalFile) throws IOException {
		InputStream fis = new FileInputStream(originalFile);
		File file = new File(originalFile.getPath() + ".checksum");
		FileOutputStream fos = new FileOutputStream(file);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		byte[] buffer = new byte[8*1024];
		int n = 0 ;
		while(n != -1) {
			n = fis.read(buffer);
			if(n>0) {
				try {
					MessageDigest digest = MessageDigest.getInstance("SHA-1");
					digest.update(buffer,0,n);
					byte [] bytes = digest.digest(); 
					bos.write(bytes,0,bytes.length);
				}catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		}
		bos.flush();
		bos.close();
		fis.close();
		return file;
	}
}

class ChunkServerHeartbeatThread extends Thread{
	ChunkServer cs;
	
	public ChunkServerHeartbeatThread(ChunkServer cs) {
		this.cs = cs;
	}
	public void sendInfo(long heartbeatCount) {  //heartbeat count decides whether we send a major or minor heartbeat
		Socket sock = null;		
		try {
			sock = new Socket(cs.CONTROLLER_HOST, cs.CONTROLLER_PORT);
		} catch (IOException e) {
			System.out.println("[Heartbeat]Could not connect to the controller. " + e.getMessage());			
		}
		try {		
			if(sock!=null) {
				if(heartbeatCount % 10 == 0) {
					System.out.println("Major Heartbeat.");
					Utils.writeStringToSocket(sock, "#MAJOR_HEARTBEAT#");
					//write chunk server info
					String server_info = InetAddress.getLocalHost().getCanonicalHostName() +","+ 
										cs.port +","+ 
										new File(Utils.STORAGE_PATH).getUsableSpace() +","+
										cs.chunkToMetaDataHash.keySet().size();
					Utils.writeStringToSocket(sock, server_info);
					for(String chunkfilename:cs.chunkToMetaDataHash.keySet()) {						
						File metaFile = cs.chunkToMetaDataHash.get(chunkfilename);
						//out.println( version + 1 ); //increment version
//					    out.println(sequence);
//					    out.println(originalFileName);
//					    out.println(System.currentTimeMillis());	
						System.out.println(metaFile);
						try(Scanner scanner = new Scanner(metaFile)){
							String chunk_info = "";
							scanner.nextLine(); //skip version
							String sequence = scanner.nextLine();
							String originalFileName = scanner.nextLine();
							scanner.nextLine(); //skip timestamp
							chunk_info = chunkfilename + "," +originalFileName + "," + sequence;
							Utils.writeStringToSocket(sock, chunk_info);
						}
					}
				}else { //minor heartbeat
					System.out.println("Minor Heartbeat.");
					Utils.writeStringToSocket(sock, "#MINOR_HEARTBEAT#");
				}
				sock.close();
			}				
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	@Override
	public void run() {
		long heartbeatCount = 0;
		while(cs.RUNNING) {
			this.sendInfo(heartbeatCount);			
			heartbeatCount++;
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}		
	}
	
}



class ChunkServerSocketThread implements Runnable {
	Socket sock;
	ChunkServer cs;
	public ChunkServerSocketThread(Socket sock, ChunkServer cs) {
		this.sock = sock;
		this.cs = cs;
	}

	public void run() {

		try {
			String action = Utils.readStringFromSocket(sock);
			if(action.equals("#SAVE_CHUNK#")) {
				String forwardChunkServers = Utils.readStringFromSocket(sock); //read arraylist of other chunk servers to forward the file to one of them
				String originalFileName = Utils.readStringFromSocket(sock);
				int sequence_no = Integer.parseInt(Utils.readStringFromSocket(sock));
				File chunkFile = Utils.readFileFromSocket(sock);
				cs.saveChunkAndForward(chunkFile,originalFileName,sequence_no,forwardChunkServers);
			}
			sock.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}



class IOThread extends Thread {
	ChunkServer cs;

	public IOThread(ChunkServer cs) {
		this.cs = cs;
	}

	@Override
	public void run() {
		Scanner scan = new Scanner(System.in);
		while (scan.hasNext()) {
			String str = scan.next();
			if (str.equals("exit")) {
				
			} else if (str.equals("print")) {
				System.out.println("--------File chunks---------");
				for(String filename:cs.fileToChunkHash.keySet()) {
					System.out.println(filename + ":" + cs.fileToChunkHash.get(filename));
				}
				System.out.println("----------------------------");
			}
			
		}
		scan.close();
	}
}

