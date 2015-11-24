package com.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import com.chunkserver.ChunkServer;

import master.Location;
import master.TFSMaster;

public class ClientFS {

	static int ServerPort = 0;
	static Socket ClientSocket;
	static ObjectOutputStream WriteOutput;
	static ObjectInputStream ReadInput;
	
	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		DestDirNotExistent, // Returned when a destination directory does not exist
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail //Returned when a method fails
	}
	
	public ClientFS()
	{
		if (ClientSocket != null) return; //The client is already connected
		try {
			BufferedReader binput = new BufferedReader(new FileReader(TFSMaster.MasterConfigFile));
			String portAndIP = binput.readLine();
			StringTokenizer str = new StringTokenizer(portAndIP,":");
			String masterIP = str.nextToken();//get the master's ip addres
			ServerPort = Integer.parseInt(str.nextToken());//get the port that master listeningon
			
			//port = port.substring( port.indexOf(':')+1 );
			//ServerPort = Integer.parseInt(port);
			ClientSocket = new Socket(masterIP,ServerPort);//should client be reading from config?
			
			WriteOutput = new ObjectOutputStream(ClientSocket.getOutputStream());
			ReadInput = new ObjectInputStream(ClientSocket.getInputStream());
			
			WriteOutput.writeObject("clientFS");
			WriteOutput.flush();
			
		}catch (FileNotFoundException e) {
			System.out.println("Error (Client), the config file "+ TFSMaster.MasterConfigFile +" containing the port of the ChunkServer is missing.");
		}catch (IOException e) {
			System.out.println("Can't find file.");
		}
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) 
	{
		try {
			//tell the master to create directory
			
			WriteOutput.writeObject("CreateDir");
			WriteOutput.flush();
			
			//write the path of src directory to server to check if it exists
			WriteOutput.writeObject(src);
			WriteOutput.flush();
			
			//if the src directory doesn't exist, return the error
			String response = (String) ReadInput.readObject();
			if (response.equals("does_not_exist")) return FSReturnVals.SrcDirNotExistent;
			
			//write the target directory name to master so it can update the namespace
			WriteOutput.writeObject(dirname);
			WriteOutput.flush();
			
			//check if directory already exists with that name
			response = (String) ReadInput.readObject();
			if (response.equals("dir_exists")) return FSReturnVals.DestDirExists;
			
			//get confirmation that the directory was created at the master namespace level
			response = (String) ReadInput.readObject();
			if (response.equals("success")) return FSReturnVals.Success;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		
		try {
			//tell the master to create directory
			WriteOutput.writeObject("DeleteDir");
			WriteOutput.flush();
			
			//write the path of src directory to server to check if it exists
			WriteOutput.writeObject(src);
			WriteOutput.flush();
			
			//if the src directory doesn't exist, return the error
			String response = (String) ReadInput.readObject();
			//System.out.println("DeleteResponse on CFS: " + response);
			if (response.equals("does_not_exist")) return FSReturnVals.SrcDirNotExistent;
			
			//write the target directory name to master so it can update the namespace
			WriteOutput.writeObject(dirname);
			WriteOutput.flush();
			
			//check if directory already exists with that name
			response = (String) ReadInput.readObject();
			//System.out.println("DeleteResponse on CFS: " + response);
			if (response.equals("dest_dir_does_not_exist")) return FSReturnVals.DestDirNotExistent;
			
			//get confirmation that the directory was deleted at the master namespace level
			response = (String) ReadInput.readObject();
			//System.out.println("DeleteResponse on CFS: " + response);
			if (response.equals("success")) return FSReturnVals.Success;
			else if (response.equals("dir_not_empty")) return FSReturnVals.DirNotEmpty;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		
		try {
			//tell the master to create directory
			WriteOutput.writeObject("RenameDir");
			WriteOutput.flush();
			
			//write the path of src directory to server to check if it exists
			WriteOutput.writeObject(src);
			WriteOutput.flush();
			
			//if the src directory doesn't exist, return the error
			String response = (String) ReadInput.readObject();
			if (response.equals("does_not_exist")) return FSReturnVals.SrcDirNotExistent;
			
			//write the new name to master so it can update the namespace
			WriteOutput.writeObject(NewName);
			WriteOutput.flush();
			
			//if there already is a directory with that name
			response = (String) ReadInput.readObject();
			if (response.equals("dest_dir_exists")) return FSReturnVals.DestDirExists;
			
			//get confirmation that the directory was renamed at the master namespace level
			response = (String) ReadInput.readObject();
			if (response.equals("success")) return FSReturnVals.Success;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		
		try {
			//tell the master to create directory
			WriteOutput.writeObject("ListDir");
			WriteOutput.flush();
			
			//write the path of src directory to server to check if it exists
			WriteOutput.writeObject(tgt);
			WriteOutput.flush();
			
			//if the src directory doesn't exist, return the error
			String response = (String) ReadInput.readObject();
			if (response.equals("does_not_exist"))
			{
				System.out.println("SRC doesn't exist, listDir CFS");
				return (new String[0]); //FSReturnVals.SrcDirNotExistent;
			}
			
			//get a server response indicating if the directory is empty
			response = (String) ReadInput.readObject();
			if(response.equals("is_empty")){
				System.out.println("SRC is empty, listDir CFS");
				return (new String[0]);
			}
			
			//get the list sent as a String[] at the master namespace level
			Vector<String> finalResponse = (Vector<String>) ReadInput.readObject();
			//since they want a string array
			String[] toReturn = new String[finalResponse.size()];
			for (int i = 0; i < finalResponse.size(); i++) 
			{
				toReturn[i] = finalResponse.elementAt(i);
			}
			return toReturn;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return new String[0];
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		
		try {
			//tell the master to create file
			WriteOutput.writeObject("CreateFile");
			WriteOutput.flush();
			
			//write the path of tgtdir directory to server to check if it exists
			WriteOutput.writeObject(tgtdir);
			WriteOutput.flush();
			
			//if the src directory doesn't exist, return the error
			String response = (String) ReadInput.readObject();
			if (response.equals("does_not_exist")) return FSReturnVals.SrcDirNotExistent;
			
			//write the target directory name to master so it can update the namespace
			WriteOutput.writeObject(filename);
			WriteOutput.flush();
			
			//check if directory already exists with that name
			//get confirmation that the directory was created at the master namespace level
			response = (String) ReadInput.readObject();
			if (response.equals("file_exists")) return FSReturnVals.FileExists;
			else if (response.equals("success")) return FSReturnVals.Success;
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		
		try {
			//tell the master to create file
			WriteOutput.writeObject("DeleteFile");
			WriteOutput.flush();
			
			//write the path of tgtdir directory to server to check if it exists
			WriteOutput.writeObject(tgtdir);
			WriteOutput.flush();
			
			//if the src directory doesn't exist, return the error
			String response = (String) ReadInput.readObject();
			if (response.equals("src_does_not_exist")) return FSReturnVals.SrcDirNotExistent;
			
			//write the target directory name to master so it can update the namespace
			WriteOutput.writeObject(filename);
			WriteOutput.flush();
			
			//check if directory already exists with that name
			//get confirmation that the directory was created at the master namespace level
			response = (String) ReadInput.readObject();
			if (response.equals("file_does_not_exist")) return FSReturnVals.FileDoesNotExist;
			else if (response.equals("success")) return FSReturnVals.Success;
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx")
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		
		ofh.setFilePath(FilePath);
		try {
			//send command to master
			WriteOutput.writeObject("OpenFile");
			WriteOutput.flush();
			
			//tell the server which file
			WriteOutput.writeObject(FilePath);
			WriteOutput.flush();
			
			String response = (String) ReadInput.readObject();
			if(response.equals("file_does_not_exist")){
				return FSReturnVals.FileDoesNotExist;
			}
			//load the list of chunks into filehandle object
			Vector<String> chunksOfFile = (Vector<String>) ReadInput.readObject();
			ofh.setHandles(chunksOfFile);
			HashMap<String, Vector<Location>> locationsOfChunks = (HashMap<String, Vector<Location>>) ReadInput.readObject();
			ofh.setLocations(locationsOfChunks);
			/*Location TestLocation = new Location("127.0.0.1", 8000);
			ofh.setPrimaryLocation(TestLocation);*/
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		return null;	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		try {
			//send command to master
			WriteOutput.writeObject("CloseFile");
			WriteOutput.flush();
			
			String filePath = ofh.getFilePath(); 
			System.out.println(filePath);
			if (filePath == null){
				System.out.println("BadHandle: fileName = null");
				return FSReturnVals.BadHandle;
			}
			Vector<String> chunksOfFile = ofh.getChunkHandles();
			if( chunksOfFile == null){
				System.out.println("BadHandle: chunks = null");
				return FSReturnVals.BadHandle;
			}
			
			WriteOutput.writeObject(filePath);
			WriteOutput.flush();
			System.out.println("Sent command to close: "+filePath);
			
			WriteOutput.writeObject(chunksOfFile);
			WriteOutput.flush();
			System.out.println("Sent chunks of: "+filePath);
			
			WriteOutput.writeObject(ofh.ChunkLocations);
			WriteOutput.flush();
			
			String response = (String) ReadInput.readObject();
			if (response.equals("success")) return FSReturnVals.Success;
				
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public void displayNamespace(){
		try{
			//ask for namespace
			WriteOutput.writeObject("NameSpace");
			WriteOutput.flush();
			
			System.out.println("");
			System.out.println("Displaying Namespace:");
			
			HashSet<String> set = (HashSet<String>) ReadInput.readObject();
			
			Iterator<String> itr = set.iterator();
	        while(itr.hasNext()){
	            System.out.println(itr.next());
	        }
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
}
