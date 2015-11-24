package master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.Vector;





import com.chunkserver.ChunkServer;
import com.chunkserver.Lease;
import com.client.FileHandle;

public class TFSMaster{
	
	public static final String MasterConfigFile = "MasterConfig.txt";
	public static final String MasterLogConfig = "MasterLogConfig.txt";
	public static String currentLogFile;
	public static Vector<String> filesThatHaveBeenDeleted;
	
	public static LinkedHashSet<String> namespace; //maps directory paths to IP address of chunk servers
	public static LinkedHashMap<String, Vector<String>> filesToChunkHandles; // maps which chunks constitute a file
	public static LinkedHashMap<String, Vector<Location>> chunkHandlesToServers; //maps chunk handles to locations of their replicas(CS IP addresses)
	public static HashMap<String, Lease> ChunkLeaseMap;
	public static HashMap<Lease, String> LeaseServerMap;
	public static HashSet<Location> connectedServers;
	
	public static final String nameSpaceFile = "namespace.txt";
	public static final String filesToChunkHandlesFile = "filesToChunkHandles.txt";
	public static int logSize = 0; public static int logNumber;
	
	public TFSMaster()
	{
		namespace = new LinkedHashSet<String>();
		filesToChunkHandles = new LinkedHashMap<String, Vector<String>>();
		chunkHandlesToServers = new LinkedHashMap<String, Vector<Location>>();
		filesThatHaveBeenDeleted = new Vector<String>();
		connectedServers = new HashSet<Location>();
		
		//read all metadata from files on startup
		readMasterLogConfig();
		readMetaData();
		ServerSocket ss = null;
		try{
			ss = new ServerSocket(0);//find open socket
			
			//write information to master's config file for chunkservers and clients to consume
			setUpConfigFile(ss);
			
			while (true)
			{
				Socket s = ss.accept();
				//System.out.println("Client connected to master");
				ServerThread st = new ServerThread(s, this);
				st.start();
			}
		}
		catch (IOException ioe) {ioe.printStackTrace();}
	}
	
	public void setUpConfigFile(ServerSocket ss){
		try{
			FileWriter fw = new FileWriter(MasterConfigFile);
			BufferedWriter bw = new BufferedWriter(fw);
			
			String masterIP = InetAddress.getLocalHost().getHostAddress();
			int portNum = ss.getLocalPort();
			
			bw.write(masterIP+":"+portNum+System.getProperty("line.separator"));
			bw.flush();
			
			bw.close();
			
		}
		catch (IOException ioe){
			System.out.println("Error setting up master's config file");
			ioe.printStackTrace();
		}
	}
	
	//this will read the masterLogConfig so the master knows which log file is the most recent
	//implements checkpointing
	public void readMasterLogConfig()
	{
		FileReader fr;
		BufferedReader br;
		try{
			fr = new FileReader(MasterLogConfig);
			br = new BufferedReader(fr);
			
			//read the number of the log file that is most current
			String logNum = br.readLine();
			this.logNumber = Integer.parseInt(logNum);
			
			//set the current log file
			this.currentLogFile = "log"+logNum+".txt";
			
			br.close();fr.close();
		}catch (FileNotFoundException e) {
			System.out.print("FNFE while reading MasterConfig info");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.print("IOE while reading MasterConfig info");
			e.printStackTrace();
		}
		
	}
	
	public void readNameSpace()
	{
		FileReader fr;
		BufferedReader br;
		
		//read namespace data
		try {
			fr = new FileReader(nameSpaceFile);
			br = new BufferedReader(fr);
			
			String temp = br.readLine();
			//if the namespacefile is empty
			if (temp == null){
				System.out.println("Namespace was empty, adding / as src");
				namespace.add("/"); //to create the overall source
				FileWriter fw = new FileWriter(nameSpaceFile,true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write("/"+System.getProperty("line.separator"));
				bw.flush();
				bw.close();
			}
			while(temp != null){
				namespace.add(temp);//add each entry
				//System.out.println("Constructor added: "+temp);
				temp = br.readLine();//read each entry from file
			}
			//System.out.println("After initialization from file namespace size: "+namespace.size());
		} catch (FileNotFoundException e) {
			System.out.print("FNFE while reading namespace info");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.print("IOE while reading namespace info");
			e.printStackTrace();
		}
	}
	public void readFilesToChunks()
	{
		FileReader fr;
		BufferedReader br;
		//read mapping of files to chunkHandles
		//each entry is in the format full/file/path.ext:chunkHandle1:chunkHandle2:chunkhandleN
		try{
				fr = new FileReader(filesToChunkHandlesFile);
				br = new BufferedReader(fr);
				String temp = br.readLine();
				while(temp != null)
				{
					StringTokenizer str = new StringTokenizer(br.readLine(),":");//read each entry from file
					String fileName = str.nextToken();//the first token should be the file name;
					Vector<String> chunksInFile = new Vector<String>();//to populate with the chunks
					while (str.hasMoreTokens())
					{
						String chunkHandle = str.nextToken();
						chunksInFile.add(chunkHandle);
					}
					//add each entry to the HashMap
					this.filesToChunkHandles.put(fileName, chunksInFile);
					temp = br.readLine();
				}
		}
		catch (FileNotFoundException e) {
			System.out.print("FNFE while reading filesToChunkHandles");
			e.printStackTrace();	
		}
		catch (IOException e) {
			System.out.print("IOE while reading filesToChunkhandles");
			e.printStackTrace();
		}	
	  }	
	
	public void readMetaData()
	{
		readNameSpace();
		readFilesToChunks();
		//readChunksToLocations();
	}
	
	public void writeFilesToChunksPersistently()
	{
		try {
			if (filesToChunkHandles.size() == 0) return;
			
			File tempFilesToChunks = new File("temp_file2chunk.txt");
			FileWriter fw = new FileWriter(tempFilesToChunks,true);
			BufferedWriter br = new BufferedWriter(fw);
			
			for (String key: filesToChunkHandles.keySet()){
				String filename = key;
				Vector<String> chunkHandles = filesToChunkHandles.get(filename);
				
				String handlesString = "";
				for (int i = 0; i < chunkHandles.size(); i++)
				{
					handlesString += chunkHandles.elementAt(i) + ":";
				}
				
				String lineToWriteToFile = key+":"+handlesString;
				//should write to file in format "file.txt:chunk1:chunk2:chunk3...
				br.write(lineToWriteToFile + System.getProperty("line.separator"));
			}
			
			File oldFilesToChunks = new File(filesToChunkHandlesFile);
			oldFilesToChunks.delete();//delete the old file
			boolean success = tempFilesToChunks.renameTo(oldFilesToChunks);//set this new version
			if (!success){
				System.out.println("Unsuccessful writing out filesToChunkHandles");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void applyLog()
	{
		if (logSize < 30)return;//don't do anything if the log not at 30 lines yet
		else System.out.println("Applying log changes from: "+currentLogFile);
		try {
			
			FileReader fr = new FileReader(currentLogFile);
			BufferedReader br = new BufferedReader(fr);
			String logLine = br.readLine();
			//read through logfile and apply operations
			//should be in format: create:srcDirectoryName:directoryToCreateName
			while (logLine!= null)
			{
				StringTokenizer str = new StringTokenizer(logLine,":");
				String command = str.nextToken();//the first token is the command
				//System.out.println("Processing: "+command);
				
				if (command.equals("createDir"))
				{
					createFromLog(str);
				}
				if (command.equals("renameDir"))
				{
					renameFromLog(str);
				}
				if (command.equals("deleteDir"))
				{
					deleteFromLog(str);
				}
				if (command.equals("createFile"))
				{
					createFileFromLog(str);
				}
				if (command.equals("deleteFile"))
				{
					deleteFilefromLog(str);
				}
				logLine = br.readLine();//read each line of the log
			}		
			
			//each time log is flushed also write out filesToChunks persistenly
			writeFilesToChunksPersistently();
			
		} catch (FileNotFoundException e) {
			System.out.println("FNFE: Error reading MasterConfig to find current log");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IOE: Error reading MasterConfig to apply current log");
			e.printStackTrace();
		}
			
		//at the end, reset the logCounter and move to new logFile
		logNumber++;//increment the lognumber after each logfile processed
		File newLog = new File("log"+logNumber+".txt");//create the new file
		this.currentLogFile = "log"+logNumber+".txt";//set it as current log
		logSize = 0;//reset the counter 
		
		//update the masterlogconfig in case of shutdown
		try {
			FileWriter fw = new FileWriter(MasterLogConfig);
			PrintWriter pw = new PrintWriter(fw);
			pw.println(logNumber);
			pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void createFromLog(StringTokenizer str)
	{
		String src = str.nextToken();
		String directoryToCreateName = str.nextToken();
		
		//create the directory in the namespace
		namespace.add(src+directoryToCreateName+"/");
		
		//add the information to the persistent namespace file
		try {
			FileWriter fw = new FileWriter(nameSpaceFile,true);//open it in append mode
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(src+directoryToCreateName+"/"+System.getProperty("line.separator"));
			bw.flush();
			
			//System.out.println("Wrote to namespacefile:" +src+directoryToCreateName+"/"+System.getProperty("line.separator"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public void deleteFromLog(StringTokenizer str)
	{
		String src = str.nextToken();
		String directoryToDelete = str.nextToken();
		
		//remove the entry from namespace in main memory
		namespace.remove(src+directoryToDelete);
		
		//apply the changes to the persistent file
		try {
			
			//update the namespace file by writing it out to temp minus the deleted namespace
			File oldnamespace = new File(nameSpaceFile);
			File newnamespace = new File("temp-namespace.txt");
			
			BufferedReader br = new BufferedReader(new FileReader(oldnamespace));
			BufferedWriter bw = new BufferedWriter(new FileWriter(newnamespace,true));//open the new file in append mode
			
			//the entry that was deleted
			String searchingFor = src+directoryToDelete;
			String currentLine;
			
			while ((currentLine = br.readLine())!= null)
			{
				if (currentLine.equals(searchingFor))continue; //skip it if its supposed to be deleted
				bw.write(currentLine+System.getProperty("line.separator"));//write with an endline separator
				bw.flush();
			}
			
			bw.close();
			br.close();
			oldnamespace.delete(); //delete the oldnamespacefile
			boolean success = newnamespace.renameTo(oldnamespace);//rename it back to the old namespace.txt file
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	public void renameFromLog(StringTokenizer str){
		
		String src = str.nextToken();
		String newName = str.nextToken();
		
		Vector<String> newNamestoAdd = new Vector<String>();
		Iterator it = namespace.iterator();
		while (it.hasNext())
		{
			String temp = (String) it.next();//iterate through each namespace entry
			if (temp.startsWith(src+"/"))
			{
				int srcLength = src.length();//get the length of the sourceDir path
				srcLength++; //to account for / character
				
				//store everything after src/
				//example - from src/a/b/c/d/e/f.txt get /b/c/d/e/f.txt
				String afterSrc = temp.substring(srcLength, temp.length());
				
				//add the renamed path
				String renamedPath = newName+"/"+afterSrc;
				newNamestoAdd.add(renamedPath);
				
				//remove the old entry from the namespace
				it.remove();
			}
		}
		
		//add back all the newly named paths to namespace
		for (int i = 0; i < newNamestoAdd.size(); i++)
		{
			namespace.add(newNamestoAdd.get(i));
		}
		
		//write out namespace to persistent file
		//update the namespace file by writing it out to temp minus the deleted namespace
		File newnamespace = new File("temp-namespace.txt");
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(newnamespace,true));//open the file in append mode
			
			it = namespace.iterator();
			while (it.hasNext())
			{
				String temp = (String) it.next();
				bw.write(temp+System.getProperty("line.separator"));//write each entry and newline char
				bw.flush();
			}
			
			bw.close();
			File oldnamespace = new File(nameSpaceFile);
			oldnamespace.delete(); //delete the oldnamespacefile
			boolean success = newnamespace.renameTo(oldnamespace);//assign the new file to namespace.txt
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	public void deleteFilefromLog(StringTokenizer str)
	{
	 try{
		String tgtdir = str.nextToken();
		String fileToDelete = str.nextToken();
		
		//update the namespace file by writing it out to temp minus the deleted namespace
		File oldnamespace = new File(nameSpaceFile);
		File newnamespace = new File("temp-namespace.txt");
		
		BufferedReader br = new BufferedReader(new FileReader(oldnamespace));
		BufferedWriter bw = new BufferedWriter(new FileWriter(newnamespace,true));//open the new file in append mode
		
		//the entry that was deleted
		String searchingFor = tgtdir+fileToDelete;
		String currentLine;
		
		while ((currentLine = br.readLine())!= null)
		{
			if (currentLine.equals(searchingFor))continue; //skip it if its supposed to be deleted
			bw.write(currentLine+System.getProperty("line.separator"));//write with an endline separator
			bw.flush();
		}
		bw.close();
		br.close();
		oldnamespace.delete(); //delete the oldnamespacefile
		boolean success = newnamespace.renameTo(oldnamespace);//rename it back to the old namespace.txt file
		
		//remove the file in mainmemory namespace
		namespace.remove(tgtdir+fileToDelete);
		
	 }catch (IOException ioe){ioe.printStackTrace();}
		
		
	}
	public void createFileFromLog(StringTokenizer str)
	{
		String src = str.nextToken();
		String newFileName = str.nextToken();
		
		//write newfile to namespace
		try {
			FileWriter fw = new FileWriter(nameSpaceFile,true);
			BufferedWriter bw = new BufferedWriter(fw);
			//add it to the namespace file
			bw.write(src+newFileName+System.getProperty("line.separator"));
			bw.flush();
			bw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		//add it to the namespace in main memory
		namespace.add(src+newFileName);
		
	}
		
	public boolean renewLease(Location loc, String ChunkHandle)
		{
			Lease lease = ChunkLeaseMap.get(ChunkHandle);
			if(loc.equals(LeaseServerMap.get(lease)))
			{
				lease.updateLeaseMaster();
				return true;
			}
			
			return false;
		}
	
	class ServerThread extends Thread
	{
		Socket s; TFSMaster master;
		ObjectInputStream ois; ObjectOutputStream oos;
		private int typeOfConnection = -1;
		private String connectedIP;
		private int connectedPort;
		//1 is a client 2 is a chunkserver
		
		public ServerThread(Socket s, TFSMaster master)
		{
			try {
				oos = new ObjectOutputStream(s.getOutputStream());
				ois = new ObjectInputStream(s.getInputStream());
				
				determineConnectionType();
				
				this.master = master;
			} catch (SocketException connectionReset){
				Location l = new Location(connectedIP, connectedPort);
				if (connectedServers.contains(l)) connectedServers.remove(l);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void determineConnectionType()
		{
			try {
				//wait for connector to tell what type
				String type = (String) ois.readObject();
				System.out.println(type + " connected to master.");
				
				if (type.equals("clientFS"))
				{
					typeOfConnection = 1;
					return;
				}
				if (type.equals("chunkserver"))
				{
					typeOfConnection = 2;
				}
				
				//get the IP address of connected client/chunkserver
				this.connectedIP = (String) ois.readObject();
				this.connectedPort = ois.readInt();
				
				
			} catch (IOException e) {
				System.out.println("Error in determining connection type");
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.out.println("Error in determining connection type");
				e.printStackTrace();
			} 
			
		}
		
		public void run()
		{
			//if it is a client connecting to master
			if (this.typeOfConnection == 1)
			{
				clientRun();
			}
			//if it is a chunkserver connecting to master
			if (this.typeOfConnection == 2)
			{
				connectedServers.add(new Location(connectedIP, connectedPort));
				sendHeartBeatMessage();
				chunkserverRun();
			}
		}
		public void clientRun()
		{
			try{
				while (true)
				{
					applyLog();
					String command = (String) ois.readObject();
					
					if (command.equals("CreateDir"))
					{
						createDir();
					}
					if (command.equals("DeleteDir"))
					{
						deleteDir();
					}
					if (command.equals("RenameDir"))
					{
						renameDir();
					}
					if (command.equals("OpenFile"))
					{
						openFile();
					}
					if (command.equals("CloseFile"))
					{
						closeFile();
					}
					if (command.equals("CreateFile"))
					{
						createFile();
					}
					if (command.equals("DeleteFile"))
					{
						deleteFile();
					}
					if (command.equals("ListDir"))
					{
						listDir();
					}
					if(command.equals("NameSpace")){
						NameSpace();
					}
					if(command.equals("getInitialLocations")){
						getInitialLocations();
					}
					
				}
			}
			catch (ClassNotFoundException e) {
					e.printStackTrace();
			}
			catch (SocketException se)
			{
				System.out.println("Connection reset exception: client disconnected from master");
			}
			catch (IOException e) {
					e.printStackTrace();
			}
		}//end client run
		public void chunkserverRun()
		{
			while (true){
				
				
				try {
					//sleep for a minute
					Thread.sleep(60000);
				}
				catch (InterruptedException ie){
					ie.printStackTrace();
				}
				
				//finds out what chunks the chunk server has, updates the namespace and metadata
				//sends back the array of deleted chunkHandles for the CS to process
				sendHeartBeatMessage();
			}
		}
		
		//finds out what chunks the chunk server has, updates the namespace and metadata
		//sends back the array of deleted chunkHandles for the CS to process
		public void sendHeartBeatMessage()
		{
			try {
				oos.writeObject("What chunks?");
				oos.flush();
				
				String hasChunks = (String) ois.readObject();
				if (hasChunks.equals("no chunks")) return;
				
				String[] chunkHandles = (String[]) ois.readObject();
				if (chunkHandles == null) {
					System.out.println("Chunks array from CS was null!");
					return;
				}
				//update the metadata
				System.out.println("Writing hearbeat message");
				for (int i = 0; i < chunkHandles.length; i++)
				{
					//check first if the chunkhandle already exists, on some other CS
					if (chunkHandlesToServers.containsKey(chunkHandles[i]))
					{
						Vector<Location> replicaLocations = chunkHandlesToServers.get(chunkHandles[i]);
						Location l = new Location(this.connectedIP, connectedPort);
						boolean add = true;
						
						//dont add the location if it already exists
						for (int x = 0; x < replicaLocations.size(); x++){
							Location compare = replicaLocations.get(x);
							if (compare.equals(l))
							{
								add = false;
							}
						}
						if (add) replicaLocations.add(l);
						continue;//no need to add an entry if it exists
								 //proceed to next iteration
					}
					
					//if the chunkhandle didn't already exist
					Location l = new Location(this.connectedIP, connectedPort);
					Vector<Location> locationsOfThisChunk = new Vector<Location>();
					locationsOfThisChunk.addElement(l);
					String handleToAdd = chunkHandles[i];
					master.chunkHandlesToServers.put(handleToAdd,locationsOfThisChunk);
				}
				oos.writeObject(filesThatHaveBeenDeleted);
				String confirmation = (String) ois.readObject();
				if (!confirmation.equals("confirmed_delete")){
					System.out.println("Error in CS processeing deleted files");
				}
				filesThatHaveBeenDeleted = new Vector<String>();//reset the vector
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
		}
		
		public void getInitialLocations(){
			Vector<Location> initialLocations = new Vector<Location>();
			//if there are three or more connected ChunkServers
			//send an arbitrary group of 3
			if (connectedServers.size()>= 3){
				Iterator it = connectedServers.iterator();
				for (int i = 0; i < 3; i++)
				{
					initialLocations.addElement((Location)it.next());
				}
			}
			//if there are less than three connected Chunkservers
			//send all of them
			else{
				Iterator it = connectedServers.iterator();
				initialLocations.addElement((Location)it.next());//add the first in case there's just one
				while (it.hasNext())//add the rest
				{
					initialLocations.addElement((Location)it.next());
				}
			}
			
			//send the vector array to the client
			try {
				oos.writeObject(initialLocations);
				oos.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
		}
		
		public void createDir() throws IOException, ClassNotFoundException
		{
			//check if the src doesn't exist
			String srcDirectory = (String) ois.readObject();
			//System.out.println("Start - Source is: " + srcDirectory);
			boolean checkSrcExists = namespace.contains(srcDirectory);//if this returns null, there is no match
			if (!checkSrcExists) {
				oos.writeObject("does_not_exist");
				oos.flush();
				System.out.println(srcDirectory +" does_not_exist");
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			
			//check if directory exists
			String dirname = (String) ois.readObject();
			//the folllowing check should fail if the directory already exists in two forms
			// src/directoryToSearch or src/directoryToSearch/ should both fail
			boolean checkDirExists = (namespace.contains(srcDirectory+"/"+dirname)||namespace.contains(srcDirectory+"/"+dirname+"/"));//if this returns null, there is no match
			if (checkDirExists) {
				oos.writeObject("dir_exists");
				oos.flush();
				System.out.println("dir_exists");
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			//append this create operation to the logfile
			if(master.currentLogFile == null) {
				System.out.println("Cannot append to log, current log == null");
				
			}
			FileWriter fw = new FileWriter(master.currentLogFile,true);//open the file in append mode
			BufferedWriter bw = new BufferedWriter(fw); 
			bw.write("createDir:"+srcDirectory+":"+dirname+System.getProperty("line.separator"));//create log record of create operation
			bw.flush();
			bw.close();
			master.logSize++;
			
			//create the directory in the namespace
			namespace.add(srcDirectory+dirname+"/");
			//System.out.println("Added: "+srcDirectory+dirname+" to namespace");
			
			//send confirmation
			oos.writeObject("success");
			oos.flush();
			
		}
		public void deleteDir()
		{
			try{
			
				//check if the src doesn't exist
				String srcDirectory = (String) ois.readObject();
				boolean checkSrcExists = namespace.contains(srcDirectory);//if this returns null, there is no match
				if (!checkSrcExists) {
					oos.writeObject("does_not_exist");
					oos.flush();
					return;
				}
				else{
					oos.writeObject(""); //because the client is still waiting for a response
					oos.flush(); //send "" to clear the readObject command in ClientFS
				}
				
				//check if directory exists
				String dirname = (String) ois.readObject();
				boolean checkDirExists = namespace.contains(srcDirectory+dirname+"/");//if this returns null, there is no match
				if (!checkDirExists) {
					System.out.println(srcDirectory+dirname + "/"+" doesn't exist");
					oos.writeObject("dest_dir_does_not_exist");
					oos.flush();
					return;
				}
				else{
					oos.writeObject(""); //because the client is still waiting for a response
					oos.flush(); //send "" to clear the readObject command in ClientFS
				}
				
				//iterate through namespace and find all matches where the src/destinationToDelete is a substring
				//this will capture all files/directories within the directory to be deleted
				int directoriesFoundToDelete = 0;
				Iterator it = (Iterator) namespace.iterator();
				while (it.hasNext())
				{
					//check if src/dest is a substring
					String toCheck = (String) it.next();
					//System.out.println("checking if: " + toCheck+ " begins w/ " + srcDirectory+dirname);
					if (toCheck.startsWith(srcDirectory+dirname))
					{
						directoriesFoundToDelete++;
					}
				}
				//send response to ClientFS
				if (directoriesFoundToDelete > 1)
				{
					oos.writeObject("dir_not_empty");
					oos.flush();
				}
				else{
					oos.writeObject("success");
					oos.flush();
					
					//write the delete to the log
					FileWriter fw = new FileWriter(currentLogFile,true);//open file in append only mode
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write("deleteDir:"+srcDirectory+":"+dirname+"/"+System.getProperty("line.separator"));
					bw.flush();
					bw.close();
					master.logSize++;
					
					//remove the namespace from directory
					namespace.remove(srcDirectory+dirname+"/");
				}
				
			}catch (IOException IOE){
				IOE.printStackTrace();
			}catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		public void renameDir() throws ClassNotFoundException, IOException
		{
			String src = (String) ois.readObject();
			//System.out.println("Master RenameDir Src: "+src);
			Iterator it;

			boolean checkSrcExists = (namespace.contains(src) || namespace.contains(src+"/"));
			if (!checkSrcExists)
			{
				oos.writeObject("does_not_exist");
				oos.flush();
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			String newName = (String) ois.readObject();
			//check if the directory already exists in the namespace
			boolean checkNewNameExists = (namespace.contains(src+"/"+newName));
			if (checkNewNameExists)
			{
				oos.writeObject("dest_dir_exits");
				oos.flush();
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			//remove the old directory from the namespace and rename
			//must also rename any directory beginning w/ src/oldName
			//System.out.println("Finding directory paths that start with: " + src);
			
			//Writes to logfile
			FileWriter fw = new FileWriter(currentLogFile,true);//open file in append only mode
			BufferedWriter bw = new BufferedWriter(fw);
			
			
			Vector<String> newNamestoAdd= new Vector<String>();
			it = namespace.iterator();
			while (it.hasNext())
			{
				String temp = (String) it.next();//iterate through each namespace entry
				if (temp.startsWith(src+"/"))
				{
					int srcLength = src.length();//get the length of the sourceDir path
					srcLength++; //to account for /
					
					//store everything after src/
					//example - from src/a/b/c/d/e/f.txt get /b/c/d/e/f.txt
					String afterSrc = temp.substring(srcLength, temp.length());
					
					//write to the log
					bw.write("renameDir:"+src+":"+newName+System.getProperty("line.separator"));
					bw.flush();
					master.logSize++;
					
					//add the renamed path
					String renamedPath = newName+"/"+afterSrc;
					newNamestoAdd.add(renamedPath);
					
					//remove the old entry from the namespace
					it.remove();
				}
			}bw.close();
			
			//add back all the newly named paths to namespace
			for (int i = 0; i < newNamestoAdd.size(); i++)
			{
				namespace.add(newNamestoAdd.get(i));
			}
			
			//send confirmation back to ClientFS
			oos.writeObject("success");
			oos.flush();

		}
		public void listDir() throws ClassNotFoundException, IOException
		{
			//receive path of directory to list contents
			String target = (String) ois.readObject();
			//check if it exists first
			boolean checkTargetExists = (namespace.contains(target)||namespace.contains(target+"/"));
			if (!checkTargetExists)
			{
				oos.writeObject("does_not_exist");
				oos.flush();
				System.out.println(target +" does_not_exist");
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			//check if the directory is empty
			Iterator it = namespace.iterator();
			//create vector of all contents of this directory
			Vector<String> contents = new Vector<String>();
			while (it.hasNext())
			{
				String temp = (String)it.next();
				if (temp.startsWith(target)&& !(temp.equals(target)||(temp.equals(target+"/") )))//if it is a match -- WONT THIS GET ALL THE SUBFOLDERS OF SUBFOLDERS
				{
					contents.addElement(temp);
				}
			}
			//if the contents only has 1 match, the directory is empty
			if(contents.size() == 1)
			{
				oos.writeObject("is_empty");
				System.out.println("the directory is empty!");
				oos.flush();
				return;
			}
			else{
				oos.writeObject("x"); //because the client is still waiting for a response
				oos.flush(); //send "" to clear the readObject command in ClientFS
			}
			
			//send the contents
			oos.writeObject(contents);
			oos.flush();
			
		}
		public void openFile()
		{
			try {
				//read which file wants to be opened
				String filePath = (String) ois.readObject();
				
				//use lookup table to get handles of all chunks of that file
				Vector<String> chunksOfFile = filesToChunkHandles.get(filePath);
				
				if(chunksOfFile == null){
					//send confirmation that file does not exist
					oos.writeObject("file_does_not_exist");
					oos.flush();
					return;
				}
				else{
					//send confirmation that file exists
					oos.writeObject("file_exists");
					oos.flush();
				}
				
				//send back to ClientFS to be loaded into the Filehandle object
				oos.writeObject(chunksOfFile);
				oos.flush();
				
				//for each chunk, get its locations on chunkservers
				//maps chunkHandles to all the locations of its replicas
				HashMap<String, Vector<Location>> ChunkLocations = new HashMap<String, Vector<Location>>();
				for (int i = 0; i < chunksOfFile.size(); i++)
				{
					Vector<Location> location = chunkHandlesToServers.get(chunksOfFile.elementAt(i));
					ChunkLocations.put(chunksOfFile.elementAt(i), location);
				}
				
				//send that array back to ClientFS to load into FileHandle object
				oos.writeObject(ChunkLocations);
				oos.flush();
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}
		public void closeFile()
		{
			try {
				//read which file wants to be opened
				String filePath = (String) ois.readObject();
				System.out.println("Read command to close: "+filePath);
				
				//read the chunks that constitute that file
				Vector<String> chunksOfFile = (Vector<String>) ois.readObject();
				System.out.println("Read chunks of: "+filePath);
				
				//remove old mapping and add new key,value pair
				System.out.println("removing "+filePath+" mapping from master.");
				filesToChunkHandles.remove(filePath);
				filesToChunkHandles.put(filePath, chunksOfFile);
				System.out.println("Updated "+filePath+" mapping from master.");
				
				//iterate through this and add it to master's namespace
				HashMap<String,Vector<Location>> locationsOfChunks = (HashMap<String,Vector<Location>>)ois.readObject();
				Iterator it = locationsOfChunks.entrySet().iterator();
				while (it.hasNext())
				{
					HashMap.Entry pair = (HashMap.Entry)it.next();
					if (!chunkHandlesToServers.containsKey(pair.getKey())){
						chunkHandlesToServers.put((String)pair.getKey(), (Vector<Location>)pair.getValue());
					}
				}
				
				oos.writeObject("success");
				oos.flush();
				
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void createFile()
		{
			try{
				//check if the src doesn't exist
				String tgtdir = (String) ois.readObject();
				
				//System.out.println("Start - Source is: " + srcDirectory);
				boolean checkSrcExists = namespace.contains(tgtdir);//if this returns null, there is no match
				if (!checkSrcExists) {
					oos.writeObject("does_not_exist");
					oos.flush();
					System.out.println(tgtdir +" does_not_exist");
					return;
				}
				else{
					oos.writeObject("x"); //because the client is still waiting for a response
					oos.flush(); //send "" to clear the readObject command in ClientFS
				}
			
			
				//check if file already exists
				String fileName = (String) ois.readObject();
				boolean checkFileExists = namespace.contains(tgtdir+fileName);//if this returns null, there is no match
				if (checkFileExists) {
					oos.writeObject("file_exists");
					oos.flush();
					System.out.println("file_exists");
					return;
				}
				else{
					//add this create file command to the log
					FileWriter fw = new FileWriter(currentLogFile,true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write("createFile:"+tgtdir+":"+fileName+System.getProperty("line.separator"));
					bw.flush();
					master.logSize++;
					
					//add the file to the namespace
					namespace.add(tgtdir+fileName);
					
					Vector<String> emptyChunkVector = new Vector<String>();
					filesToChunkHandles.put(tgtdir+fileName, emptyChunkVector);
					
					//send confirmation
					oos.writeObject("success");
					oos.flush();
				}
			
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
			}
		}
		public void deleteFile()
		{
			try{
				//check if the src doesn't exist
				String tgtdir = (String) ois.readObject();
				
				//System.out.println("Start - Source is: " + srcDirectory);
				boolean checkSrcExists = namespace.contains(tgtdir);//if this returns null, there is no match
				if (!checkSrcExists) {
					oos.writeObject("src_does_not_exist");
					oos.flush();
					System.out.println(tgtdir +" src_does_not_exist");
					return;
				}
				else{
					oos.writeObject("x"); //because the client is still waiting for a response
					oos.flush(); //send "" to clear the readObject command in ClientFS
				}
			
			
				//check if file already exists
				String fileName = (String) ois.readObject();
				boolean checkFileExists = namespace.contains(tgtdir+fileName);//if this returns null, there is no match
				if (!checkFileExists) {
					oos.writeObject("file_does_not_exist");
					oos.flush();
					System.out.println("file_does_not_exist");
					return;
				}
				else{
					//add this delete file command to the log
					FileWriter fw = new FileWriter(currentLogFile,true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write("deleteFile:"+tgtdir+":"+fileName+System.getProperty("line.separator"));
					bw.flush();
					master.logSize++;
					
					//remove the file from the namespace
					namespace.remove(tgtdir+fileName);
					
					//send confirmation
					oos.writeObject("success");
					oos.flush();
				}
			
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
			}
		}
		public void NameSpace() {
			//send confirmation
			try {
				//send namespace hashset
				oos.writeObject(namespace);
				oos.flush();
				
			} catch (IOException e) {
				e.printStackTrace();
		}
			
		}
	}
	
	public static void main(String[] args){
		TFSMaster master = new TFSMaster();
	}



}
