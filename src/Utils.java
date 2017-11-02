import java.io.*;
import java.net.*;

public class Utils {
	
	public static String STORAGE_PATH = "/tmp/tarequl/";
	
	public static String readStringFromSocket(Socket sock) throws IOException {
		ObjectInputStream OIS = new ObjectInputStream(sock.getInputStream());
		try {
			return (String)OIS.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}	
	
	public static void writeStringToSocket(Socket sock, String message) throws IOException {
		ObjectOutputStream OOS = new ObjectOutputStream(sock.getOutputStream());
		OOS.writeObject(new String(message));
	}
	
	public static Object readObjectFromSocket(Socket sock) throws IOException, ClassNotFoundException {
		ObjectInputStream OIS = new ObjectInputStream(sock.getInputStream());
		return OIS.readObject();
	}
	
	public static void writeObjectToSocket(Socket sock, Object obj) throws IOException {
		ObjectOutputStream OOS = new ObjectOutputStream(sock.getOutputStream());
		OOS.writeObject(obj);
	}
	
	/**
	 * Reads a file from socket, saves in the /tmp directory, returns the filename
	 * @param sock
	 * @return filename
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	
	public static File readFileFromSocket(Socket sock) throws IOException, ClassNotFoundException{
		ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
		String filename = (String)ois.readObject();
		System.out.println("Received file "+ filename);
		File file = new File(STORAGE_PATH+filename);
		FileOutputStream fos = new FileOutputStream(file);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		
		int buff_size = 10000;
		byte[] contents;
		long filesize = ois.readLong();
		long iterations = filesize/buff_size + ((filesize%buff_size>0)?1:0);
		for(int i = 0 ; i < iterations; i++) {
			contents = (byte[])ois.readObject();
			bos.write(contents,0,contents.length);
		}		
		bos.flush();
		bos.close();
		return file;
	}
	
	public static void writeFileToSocket(Socket sock, File file) throws IOException{
		FileInputStream fis = new FileInputStream(file);
		BufferedInputStream bis = new BufferedInputStream(fis);		
		ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
		byte[] contents;
		long fileLength = file.length();
		long current = 0;
		oos.writeObject(new String(file.getName()));
		oos.writeLong(new Long(fileLength));		
		while(current!=fileLength) {
			int size = 10000;
			if(fileLength - current >= size) {
				current += size;
			}else { 
                size = (int)(fileLength - current); 
                current = fileLength;
            } 
            contents = new byte[size]; 
            
            bis.read(contents, 0, size); 
            oos.writeObject(contents);
		}
		System.out.println("Sending file "+ file.getName());
        bis.close();
	}
	

}
