import java.util.*;
import java.io.*;
import java.net.*;

public class Client{
	
	String CONTROLLER_HOST;
	int CONTROLLER_PORT;
	
	ArrayList<File> fileChunks;
	String filename;
	
	String tempCS = "albany.cs.colostate.edu";
	int tempCSport = 57890;
	
	
	
	
	public static void main(String[] args) {
		new File(Utils.STORAGE_PATH).mkdirs();
		String filename="";
		String action="";
		if (args.length > 1) {
			action = args[0];
			filename = args[1];			
		} 
		if(args.length < 2 || filename.length()==0 || (action.equalsIgnoreCase("PUT")==false && action.equalsIgnoreCase("GET")==false)){
			System.out.println("Usage: java Client <PUT/GET> <filename>");
			return;
		}
		new Client(action,filename);
	}
	
	public Client(String action, String filename) {
		this.filename = filename;
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
       
        
        if(action.equalsIgnoreCase("PUT")) {
        	this.sendFile();
        }else {
        	this.getFile();
        }
        
	}
	
	private void getFile() {
		try {
			Socket sock = new Socket(CONTROLLER_HOST, CONTROLLER_PORT);
			Utils.writeStringToSocket(sock, "#CHUNKLIST#");
			Utils.writeStringToSocket(sock, filename);
			String isFound = Utils.readStringFromSocket(sock);
			if(isFound.equals("$FILE_NOT_FOUND$")) {
				System.out.println("File not found.");
				sock.close();
			}else {
				String chunkNames = Utils.readStringFromSocket(sock);
				sock.close();
				String[] chunkNameArr = chunkNames.split(",");
				Arrays.sort(chunkNameArr);
				System.out.println(Arrays.toString(chunkNameArr));
				File[] fileArr = new File[chunkNameArr.length];
				boolean fileCorrupted = false;
				//get these chunks
				for(int i = 0 ;i < chunkNameArr.length; i++) {
					fileArr[i] = getChunk(chunkNameArr[i]);
					if(fileArr[i]==null) {
						fileCorrupted = true;						
						break;
					}
				}

				if(fileCorrupted == true) {
					System.out.println("File Corrupted!");
				}else {
					//TODO: Put together the chunks to make the file	
					try(BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("1_"+filename))){
						for(int i = 0 ; i < fileArr.length; i++) {
							System.out.println(i);
				        	try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileArr[i]))){
				        		byte[] buffer = new byte[(int)fileArr[i].length()];
				        		bis.read(buffer);
				        		bos.write(buffer);
				        	}
				        }
					}
			        
					
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private File getChunk(String chunkName) {
		try {
			Socket sock = new Socket(CONTROLLER_HOST, CONTROLLER_PORT);
			Utils.writeStringToSocket(sock, "#GET_SERVERS_FOR_A_CHUNK#");
			Utils.writeStringToSocket(sock, chunkName);
			String chunkServers = Utils.readStringFromSocket(sock);
			System.out.println("Servers containing the chunk "+chunkName+": " + chunkServers);
			sock.close();
			String[] serverList = chunkServers.split(",");
			
			String[] nextServer = serverList[0].split(":");
			chunkServers = "";
			if(serverList.length > 1) {
				chunkServers = serverList[1];
				for(int i = 2; i < serverList.length; i++) {
					chunkServers += ","+serverList[i];
				}
			}
			
			//get the chunk from first chunk server
			Socket sock1= new Socket(nextServer[0], Integer.parseInt(nextServer[1]));
			Utils.writeStringToSocket(sock1, "#GET_CHUNK#");
			Utils.writeStringToSocket(sock1, chunkServers);
			Utils.writeStringToSocket(sock1, chunkName);
			String isFileOK = Utils.readStringFromSocket(sock1);
			if(isFileOK.equals("$FILE_OK$")) {
				File chunkFile = Utils.readFileFromSocket(sock1);
				return chunkFile;
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}		
		return null;
	}
	
	private void sendFile() {
		
        try {
			this.fileChunks = splitFile(new File(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
        if(this.fileChunks != null) {
        	System.out.println(this.fileChunks);
        }
		
		for(int i = 0 ; i < this.fileChunks.size(); i++) {		
			File f = this.fileChunks.get(i);
			try {
				Socket sock = new Socket(CONTROLLER_HOST, CONTROLLER_PORT);
				Utils.writeStringToSocket(sock, "#REQUEST_FREE_CHUNK_SERVER#");
				//send the chunk trying to save; it'll check if the chunk already exists
				//if it does, then the servers list will be for that specific chunk, so that the chunk is overwritten.
				Utils.writeStringToSocket(sock, f.getName());  
				String chunkServers = Utils.readStringFromSocket(sock);
				System.out.println("Servers List: " + chunkServers);
				sock.close();
				String[] serverList = chunkServers.split(",");
				
				String[] nextServer = serverList[0].split(":");
				chunkServers = "";
				if(serverList.length > 1) {
					chunkServers = serverList[1];
					for(int k = 2; k < serverList.length; k++) {
						chunkServers += ","+serverList[k];
					}
				}
				//send to chunk server
				
				Socket sock1 = new Socket(nextServer[0], Integer.parseInt(nextServer[1]));	
				Utils.writeStringToSocket(sock1, "#SAVE_CHUNK#");
				Utils.writeStringToSocket(sock1, chunkServers); //send ArrayList of other chunks to forward the file to
				Utils.writeStringToSocket(sock1, this.filename);
				Utils.writeStringToSocket(sock1, i+"");
				Utils.writeFileToSocket(sock1, f);	
				sock1.close();
			}catch (IOException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
    private ArrayList<File> splitFile(File f) throws IOException {
        int partCounter = 1;

        int sizeOfFiles = 64 * 1024;// 64KB
        byte[] buffer = new byte[sizeOfFiles];
        
        ArrayList<File> files = new ArrayList<>();

        String fileName = f.getName();

        //try-with-resources to ensure closing stream
        try (FileInputStream fis = new FileInputStream(f);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data into separate file with different number in name
                String filePartName = String.format("%s_chunk%03d", fileName, partCounter++);
                File newFile = new File(Utils.STORAGE_PATH, filePartName);
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    out.write(buffer, 0, bytesAmount);
                }
                files.add(newFile);
            }
        }
        
        return files;
    }
}