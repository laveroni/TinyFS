package master;

import java.io.Serializable;

public class Location implements Serializable{

	public String IPAddress;
	public int port;
	public Location(String IPAddress, int port)
	{
		this.IPAddress = IPAddress;
		this.port = port;
	}
	
	public boolean equals(Location other)
	{
		return (other.IPAddress == this.IPAddress && other.port == this.port);
	}
}
