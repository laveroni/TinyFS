package com.chunkserver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.client.Client;
import com.client.RID;

public class CStoCSThread extends Thread
{
	//Commands recognized by the Server
		public static final int CreateChunkCMD = 201;
		public static final int WriteChunkCMD = 202;
		public static final int AppendRecord = 203;
		public static final int DeleteRecord = 204;
		public static final int CloseSockets = 205;
		
		//Replies provided by the server
		public static final int TRUE = 1;
		public static final int FALSE = 0;
		
		private ChunkServer cs;
		private Socket CSConnection;
		private ObjectOutputStream WriteOutput;
		private ObjectInputStream ReadInput;
		
		public CStoCSThread(ChunkServer cs, Socket s, ObjectInputStream ois, ObjectOutputStream oos)
		{
			this.cs = cs;
			this.CSConnection = s;
			this.ReadInput = ois;
			this.WriteOutput = oos;
		}
		
		public void run()
		{
			try {
				int offset;
				int payloadlength;
				int chunkhandlesize;
				byte[] CHinBytes;
				byte[] payload;
				RID rid;
				String ChunkHandle;
				int CMD = 0;
				int size;
				int counter;
				//Use the existing input and output stream as long as the client is connected
				while (!CSConnection.isClosed()) {
					CMD = ChunkServer.ReadIntFromInputStream("CStoCS0", ReadInput);
					switch (CMD){
					case CreateChunkCMD:
						String chunkhandle = cs.createChunk();
						byte[] CHinbytes = chunkhandle.getBytes();
						size = ChunkServer.ReadIntFromInputStream("CStoCS", ReadInput);
						CHinBytes = ChunkServer.RecvPayload("CStoCS", ReadInput, size);
						counter = Integer.parseInt(new String(CHinBytes));
						cs.setCounter(counter);
						WriteOutput.writeInt(1);
						WriteOutput.flush();
						break;

					case WriteChunkCMD:
						
						payloadlength =  ChunkServer.ReadIntFromInputStream("ClientInstance3", ReadInput);
						payload = ChunkServer.RecvPayload("ChunkServer", ReadInput, payloadlength);
						chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance3", ReadInput);
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
						CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();
				
						//Call the writeChunk command
						cs.append(ChunkHandle, payload);
						WriteOutput.writeInt(1);
						WriteOutput.flush();
						break;
					case DeleteRecord:
						int recordIndex = ChunkServer.ReadIntFromInputStream("ClientInstance4", ReadInput);		
						chunkhandlesize = ChunkServer.ReadIntFromInputStream("ClientInstance4", ReadInput);
						CHinBytes = ChunkServer.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();	
						cs.deleteRecord(ChunkHandle, recordIndex);
						WriteOutput.writeInt(1);
						
						WriteOutput.flush();
						break;
					}
				}
			}
			catch(IOException ioe)
			{
				
			} 
			finally {
				try {
					if (CSConnection != null && !CSConnection.isClosed())
						CSConnection.close();
					if (ReadInput != null)
						ReadInput.close();
					if (WriteOutput != null) WriteOutput.close();
				} catch (IOException fex){
					System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
					fex.printStackTrace();
				}
			}
		}
}
