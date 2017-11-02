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
        
        try {
			this.fileChunks = splitFile(new File(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
        if(this.fileChunks != null) {
        	System.out.println(this.fileChunks);
        }
        
        this.sendFiles();
	}
	
	private void sendFiles() {
		for(int i = 0 ; i < this.fileChunks.size(); i++) {		
			File f = this.fileChunks.get(i);
			//send to chunk server
			try {
				String forwardChunkServers = "boise.cs.colostate.edu:57890,denver.cs.colostate.edu:57890";
				Socket sock = new Socket(tempCS, tempCSport);	
				Utils.writeStringToSocket(sock, "#SAVE_CHUNK#");
				Utils.writeStringToSocket(sock, forwardChunkServers); //send ArrayList of other chunks to forward the file to
				Utils.writeStringToSocket(sock, this.filename);
				Utils.writeStringToSocket(sock, i+"");
				Utils.writeFileToSocket(sock, f);	
				sock.close();
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
        int partCounter = 1;//I like to name parts from 001, 002, 003, ...
                            //you can change it to 0 if you want 000, 001, ...

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
                String filePartName = String.format("%s_chunk%d", fileName, partCounter++);
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