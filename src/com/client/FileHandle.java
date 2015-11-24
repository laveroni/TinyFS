package com.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import master.Location;

public class FileHandle {
	
	String filePath;
	Vector<String> ChunkHandles; //array of all chunk handles that constitute the file
	HashMap<String, Vector<Location>> ChunkLocations;
	boolean newChunk = true;

	
	public FileHandle(){
		ChunkHandles = new Vector<String>();
		ChunkLocations = new HashMap<String, Vector<Location>>();
	}
	
	public void setFilePath(String path)
	{
		this.filePath = path;
	}

	public String getFilePath()
	{
		return this.filePath;
	}
	public void setHandles(Vector<String> chunksOfFile)
	{
		this.ChunkHandles = chunksOfFile;
	}
	
	public Vector<String> getChunkHandles()
	{
		return ChunkHandles;
	}
	
	public void setLocations(HashMap<String, Vector<Location>> locationsOfChunks)
	{
		this.ChunkLocations = locationsOfChunks;
	}
	
	public HashMap<String, Vector<Location>> getLocations()
	{
		return ChunkLocations;
	}
	
	public void setNewChunk(boolean newChunk)
	{
		this.newChunk = newChunk;
	}
	
	public boolean getNewChunk()
	{
		return newChunk;
	}

	
}
