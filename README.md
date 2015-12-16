# TinyFS

This TinyFS project is a Java implementation of the Google File System.  

* Note, this is a conceptual and educational implementation of the Google File System that is not intended (at this time) for actual use. 

To read about the Google File System, visit the following link:  http://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf

This implementation consists of the ability to run multiple chunkservers, multiple clients, and a single master that connects the clients with a chunkserver.  

To run:
Start the master, enter the master's assigned IP and port into the masterconfig.txt file for the chunkservers and clients, start at least one chunkserver, and then start the unit tests which act as the client.  
