package com.chunkserver;

public class Lease {

	private Long timeToExpire;
	private String chunkHandle;
	
	public Lease(String chunkHandle)
	{
		this.chunkHandle = chunkHandle;
		timeToExpire = System.currentTimeMillis() + 3000; //55 seconds from creation
	
	}
	
	public boolean needsRenewal()
	{
		if(System.currentTimeMillis() > timeToExpire){
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public String getChunkHandle()
	{
		return chunkHandle;
	}
	public void updateLeaseMaster()
	{
		timeToExpire = System.currentTimeMillis() + 3600;
	}
	
	public void updateLeaseCS()
	{
		timeToExpire = System.currentTimeMillis() + 3300;
	}
}
