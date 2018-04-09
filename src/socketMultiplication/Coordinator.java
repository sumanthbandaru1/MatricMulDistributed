package socketMultiplication;

import java.io.*;  
import java.util.Arrays;

import matrix.*; 

public class Coordinator implements Runnable{ 
	
	Connection conn; 
	public int[][] a; 
	public int[][] b; 
	public static int[][] c; 
	
	
	
	public static int[][] e;
	
	
	int numNodes; 
	DataInputStream[][] disWorkers;
	DataOutputStream[][] dosWorkers; 
	int m;
	int sqrtP;
	int port;
	int n;
	
	
	
	public Coordinator(int [][] a,int [][]b, int numNodes,int port) {
		int n=a.length;
		this.a = a; 
		this.b = b;
		if(a.length!=b.length) throw new Error("Only supports multiplication of two matrices with same dimension");
		if(a.length<1||b.length<1) throw new Error("Matrix dimension should be atleast 1");
		if(a.length!=a[0].length||b.length!=b[0].length)throw new Error("Only supports multiplication of square matrices");
		this.n=n;
		c = new int[n][n]; 
		
		this.numNodes = numNodes; 
		
		double sqrtP=Math.sqrt(numNodes);
		if(Math.round(sqrtP)!=sqrtP) throw new Error("Number of Nodes(p) should be perfect square");
		
		
		double m=n/sqrtP;
		if(Math.round(m)!=m) throw new Error("M doesnot satisfy m = n/sqrtP ");
		
		
		this.m=(int)Math.round(m);
		this.sqrtP=(int)Math.round(sqrtP);
		this.port=port;
	}
	
	void configurate()
	{
		this.configurate(port);
	}
 	void configurate(int portNum) { 
		try { 
			
			System.out.println("Random Matrix A");
			MatrixMultiple.displayMatrix(a);
			
			//do initial shifting on a and b
			System.out.println("Random Matrix B");
			MatrixMultiple.displayMatrix(b);
			
			//performing the initial shift
			System.out.println("Shift Matrix A");
			for(int i=0;i<n;i++)
			{
				int temp[]=Arrays.copyOf(a[i], a[i].length);
				for(int j=0;j<n;j++)
				{
					
					a[i][(j+n-(i+1))%n]=temp[j];
				}
			}
			System.out.println("Shifted Matrix A");
			MatrixMultiple.displayMatrix(a);
			// initial shifting on a and b
			
			System.out.println("Shift Matrix B");
			for(int i=0;i<n;i++)
			{
				int temp[]=new int [n];
				for(int j=0;j<n;j++)
				{
					temp[j]=b[j][i];
					
				}
				for(int j=0;j<n;j++)
				{
					b[(j+n-(i+1))%n][i]=temp[j];
					
				}
			}
					
			System.out.println("Shifted Matrix B");
			MatrixMultiple.displayMatrix(b);
			
			conn = new Connection(portNum); 
			disWorkers = new DataInputStream[sqrtP][sqrtP]; 
			dosWorkers = new DataOutputStream[sqrtP][sqrtP];
			String[][] ips = new String[sqrtP][sqrtP]; 
			int[][] portsX = new int[sqrtP][sqrtP];
			int[][] portsY = new int[sqrtP][sqrtP];
			for (int i=0; i<numNodes; i++ ) { 
				DataIO dio = conn.acceptConnect(); 
				DataInputStream dis = dio.getDis(); 
				int nodeXNum = dis.readInt(); 			
				int nodeYNum = dis.readInt();
				
				
				ips[nodeXNum][nodeYNum] = dis.readUTF(); 			//get worker ip
				
				portsX[nodeXNum][nodeYNum] = dis.readInt();  		//get worker port #
				portsY[nodeXNum][nodeYNum] = dis.readInt();  		//get worker port #
				disWorkers[nodeXNum][nodeYNum] = dis; 
				dosWorkers[nodeXNum][nodeYNum] = dio.getDos(); 	//the stream to worker ID
				dosWorkers[nodeXNum][nodeYNum].writeInt(n); 		//assign matrix dimension (height)
				dosWorkers[nodeXNum][nodeYNum].writeInt(m); 	//assign matrix width 
			}
			System.out.printf("All workers Joined, Sending Neighbour information to all workers\n");
			for(int i=0;i<sqrtP;i++)
				for(int j=0;j<sqrtP;j++)
				{
					int myLeft=(j+sqrtP-1)%sqrtP;
					int myRight=(j+1)%sqrtP;
					int myTop=(i+sqrtP-1)%sqrtP;
					int myBottom=(i+1)%sqrtP;
					dosWorkers[i][j].writeUTF(ips[i][myLeft]);
					dosWorkers[i][j].writeInt(portsX[i][myLeft]);
					
					dosWorkers[i][j].writeUTF(ips[i][myRight]);
					dosWorkers[i][j].writeInt(portsX[i][myRight]);
					
					dosWorkers[i][j].writeUTF(ips[myTop][j]);
					dosWorkers[i][j].writeInt(portsY[myTop][j]);
					
					dosWorkers[i][j].writeUTF(ips[myBottom][j]);
					dosWorkers[i][j].writeInt(portsY[myBottom][j]);
					
				}
			System.out.printf("Sent neighbour information to all the workers\n");
			
		} catch (IOException ioe) { 
			System.out.println("error: Coordinator assigning neighbor infor.");  
			ioe.printStackTrace(); 
		} 
	}
	
	void distribute() { 
		for(int i=0;i<sqrtP;i++)
			for(int j=0;j<sqrtP;j++)
			{
				System.out.printf("Sending %d*%d sized block of Matrix A to worker [%d,%d]\n",m,m,i,j);
				for(int k=0;k<m;k++)
					for(int l=0;l<m;l++)
					{
						try {
							dosWorkers[i][j].writeInt(a[i*m+k][j*m+l]);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							throw new Error("Error sending initial data");
						}
					}
			}
		for(int i=0;i<sqrtP;i++)
			for(int j=0;j<sqrtP;j++)
			{
				System.out.printf("Sending %d*%d sized block of Matrix B to worker [%d,%d]\n",m,m,i,j);
				for(int k=0;k<m;k++)
					for(int l=0;l<m;l++)
					{
						try {
							dosWorkers[i][j].writeInt(b[i*m+k][j*m+l]);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							throw new Error("Error sending initial data");
						}
					}
			}
		System.out.printf("Waiting For result matrix\n");
		int [][]result=new int[n][n];
		for(int i=0;i<sqrtP;i++)
			for(int j=0;j<sqrtP;j++)
			{
				for(int k=0;k<m;k++)
					for(int l=0;l<m;l++)
					{
						try {
							result[i*m+k][j*m+l]=disWorkers[i][j].readInt();
						} catch (IOException e) {
							e.printStackTrace();
							throw new Error("Error reading output");
						}
						
					}
			}
		
		c=result;
	}
	
	public static void main(String[] args) { 
		if (args.length != 3) {
			System.out.println("usage: java Coordinator maxtrix-dim number-nodes coordinator-port-num");
			System.exit(1);
		} 
		int matrixdim = Integer.parseInt(args[0]);
		int numNodes = Integer.parseInt(args[1]);
		int portnum = Integer.parseInt(args[2]);
		
		//create Random Matrix
		int [][] matA = MatrixMultiple.createDisplayMatrix(matrixdim);
		int [][] matB = MatrixMultiple.createDisplayMatrix(matrixdim);
		
		int [] [] matResult = MatrixMultiple.multiplyMatrix(matA, matB);
		
		
		//setup self
		Coordinator coor = new Coordinator(matA,matB,numNodes,portnum);
		coor.run();
		System.out.println("Comparing now\n");
		System.out.println("Result using Normal Multiplication\n");
		MatrixMultiple.displayMatrix(matResult);
		System.out.println("Result using Distributed Multiplication\n");
		MatrixMultiple.displayMatrix(c);
		if(MatrixMultiple.compareMatrix(matResult, c)) System.out.println("Identical.");
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		this.configurate();
		this.distribute();
		//System.out.println("Result\n");
		MatrixMultiple.displayMatrix(c);
		System.out.println("Computation Done");
	}
}
