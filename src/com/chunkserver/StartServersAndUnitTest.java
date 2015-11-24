package com.chunkserver;

import UnitTests3.UnitTest1;
import UnitTests3.UnitTest2;
import UnitTests3.UnitTest3;
import UnitTests3.UnitTest4;
import UnitTests3.UnitTest5;
import UnitTests3.UnitTest6;
import master.TFSMaster;

public class StartServersAndUnitTest
{

	public static void main(String [] args)
	{
		
		ChunkServer cs = new ChunkServer();
		cs.start();
		//UnitTest1.main(args);
		//UnitTest2.main(args);
		//UnitTest3.main(args);
		//UnitTest4.main(args);
		//UnitTest5.main(args);
		/*try
		{
			UnitTest6.main(args);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}*/
	}
}
