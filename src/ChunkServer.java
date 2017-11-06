import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;
import java.net.*;
import java.security.*;

public class ChunkServer extends Thread{
	
	ExecutorService threadPool = Executors.newFixedThreadPool(20);
	
	HashMap<String, Set<File>> filenameToChunkMap; //hashmap: filename -> list of chunk files
	
	HashMap<String, File> chunknameToFileMap; //hashmap: chunk fileanme -> file handle for metadata
	HashMap<String, File> chunknameToMetaDataMap; //hashmap: chunk fileanme -> file handle for metadata
	HashMap<String, File> transientChunknametoMetaDataMap; //hashmap: chunk filename -> file handle for metadata
	HashMap<String, File> chunknameToChecksumMap; //hashmap: chunk fileanme -> file handle for checksum
	
	
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
		this.filenameToChunkMap = new HashMap<>();
		this.chunknameToFileMap = new HashMap<>();
		this.transientChunknametoMetaDataMap = new HashMap<>();
		this.chunknameToChecksumMap = new HashMap<>();
		this.chunknameToMetaDataMap = new HashMap<>();
		
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
		Set<File> chunks = filenameToChunkMap.get(originalFileName);
		if(chunks == null) {
			chunks = new HashSet<>();			
		}
		chunks.add(chunkFile);
		filenameToChunkMap.put(originalFileName, chunks);
		
		chunknameToFileMap.put(chunkFile.getName(), chunkFile);
		
		File checksumFile = this.createChecksumFile(chunkFile);
		chunknameToChecksumMap.put(chunkFile.getName(),checksumFile);
		
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
		chunknameToMetaDataMap.put(chunkFile.getName(),metaFile);
		//track new files; will be removed as soon as pushed to the controller
		transientChunknametoMetaDataMap.put(chunkFile.getName(), metaFile);
		
				
		//forward the chunk to the next chunk server
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
	
	private File createChecksumFile(File chunkFile) throws IOException {
		InputStream fis = new FileInputStream(chunkFile);
		File file = new File(chunkFile.getPath() + ".checksum");
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
	
	public boolean checkChunkForCorruption(String chunkName) throws IOException {
		File chunkFile = chunknameToFileMap.get(chunkName);
		InputStream chunkFIS = new FileInputStream(chunkFile);
		
		File checksumFile = new File(chunkFile.getPath() + ".checksum");
		InputStream checksumFIS = new FileInputStream(checksumFile);
		
		boolean isCorrupted = false;
		byte[] buffer = new byte[8*1024];
		int n = 0 ;
		while(n != -1) {
			n = chunkFIS.read(buffer);
			if(n>0) {
				try {
					MessageDigest digest = MessageDigest.getInstance("SHA-1");
					digest.update(buffer,0,n);
					byte [] bytes = digest.digest(); 
					byte [] storedBytes = new byte[bytes.length];
					checksumFIS.read(storedBytes);
					if(Arrays.equals(bytes, storedBytes) == false) {
						isCorrupted = true;						
					}
					
				}catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		}	
		//make sure that we reached the end of stored checksum file
		n = checksumFIS.read(buffer);
		if(n>0) {
			isCorrupted = true;
		}
		
		chunkFIS.close();
		checksumFIS.close();
		
		return isCorrupted;
	}
}

class ChunkServerHeartbeatThread extends Thread{
	ChunkServer cs;
	
	public ChunkServerHeartbeatThread(ChunkServer cs) {
		this.cs = cs;
	}
	public boolean sendInfo(long heartbeatCount) {  //heartbeat count decides whether we send a major or minor heartbeat
		Socket sock = null;		
		try {
			sock = new Socket(cs.CONTROLLER_HOST, cs.CONTROLLER_PORT);
		} catch (IOException e) {
			System.out.println("[Heartbeat]Could not connect to the controller. " + e.getMessage());			
		}
		try {		
			if(sock!=null) {
				//write chunk server info
				String server_info = InetAddress.getLocalHost().getCanonicalHostName() +","+ 
									cs.port +","+ 
									new File(Utils.STORAGE_PATH).getUsableSpace() +","+
									cs.chunknameToMetaDataMap.keySet().size();
				if(heartbeatCount % 10 == 0) {
					System.out.println("Major Heartbeat.");
					Utils.writeStringToSocket(sock, "#MAJOR_HEARTBEAT#");
					Utils.writeStringToSocket(sock, server_info);
					sendChunkMetadata(sock, cs.chunknameToMetaDataMap);
				}else { //minor heartbeat
					System.out.println("Minor Heartbeat.");
					Utils.writeStringToSocket(sock, "#MINOR_HEARTBEAT#");
					Utils.writeStringToSocket(sock, server_info);
					
					//send how many new chunks
					Utils.writeStringToSocket(sock, cs.transientChunknametoMetaDataMap.size() +"");
					//send the new chunks
					sendChunkMetadata(sock, cs.transientChunknametoMetaDataMap);
					
										
					boolean request_major_heartbeat = Boolean.parseBoolean(Utils.readStringFromSocket(sock));
					return request_major_heartbeat;
				}
				sock.close();
			}				
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return false;
	}
	private void sendChunkMetadata(Socket sock, HashMap<String, File> hash) throws IOException, FileNotFoundException {
		for(String chunkfilename:hash.keySet()) {	
			//TEST----------------
			//System.out.println("checking for corruption.");
//			if(cs.checkChunkForCorruption(chunkfilename)) {
//				System.out.println("Corruption detected in file "+ chunkfilename);
//			}
			//--------------------
			File metaFile = cs.chunknameToMetaDataMap.get(chunkfilename);			
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
		cs.transientChunknametoMetaDataMap.clear(); //clear all temp data	
	}
	@Override
	public void run() {
		long heartbeatCount = 0;
		while(cs.RUNNING) {
			boolean request_major_heartbeat = this.sendInfo(heartbeatCount);
			if(request_major_heartbeat == false) {
				heartbeatCount++;
				try {
					Thread.sleep(5 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else { // request major heartbeat
				heartbeatCount = 0;
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
			}else if(action.equals("#GET_CHUNK#")) {
				String forwardChunkServers = Utils.readStringFromSocket(sock); //read arraylist of other chunk servers to check file corruption
				String chunkName = Utils.readStringFromSocket(sock);
				if(cs.checkChunkForCorruption(chunkName)) {
					Utils.writeStringToSocket(sock, "$FILE_CORRUPTED$");
				}else {
					Utils.writeStringToSocket(sock, "$FILE_OK$");
					File chunkFile = cs.chunknameToFileMap.get(chunkName);
					Utils.writeFileToSocket(sock, chunkFile);
				}
				//TODO: Check the other servers for corruption
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
				for(String filename:cs.filenameToChunkMap.keySet()) {
					System.out.println(filename + ":" + cs.filenameToChunkMap.get(filename));
				}
				System.out.println("----------------------------");
			}
			
		}
		scan.close();
	}
}

