package socketMultiplication;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

import matrix.*;

public class Worker implements Runnable {

	int nodeXNum;
	int nodeYNum;
	int localXPort;
	int localYPort;
	Connection connX;
	Connection connY;
	int m;
	int n;
	int[][] a;
	int[][] b;
	int[][] c;
	DataInputStream disCoor;
	DataOutputStream dosCoor;

	DataOutputStream dosLeft;
	DataOutputStream dosTop;

	DataInputStream disRight;
	DataInputStream disBottom;
	String coordinatorIP;

	String LocalIp;

	int coordinatorPort;

	public Worker(int nodeXNum, int nodeYNum, int localXPort, int localYPort, String coordinatorIP, int coordinatorPort,
			String localip) {
		this.nodeXNum = nodeXNum;
		this.nodeYNum = nodeYNum;
		this.localXPort = localXPort;
		this.localYPort = localYPort;
		this.coordinatorIP = coordinatorIP;
		this.coordinatorPort = coordinatorPort;
		this.LocalIp = localip;
	}

	void configurate() {
		this.configurate(coordinatorIP, coordinatorPort);
	}

	void configurate(String coorIP, int coorPort) {
		try {
			System.out.printf("Configuring Worker [%d,%d]\n", nodeXNum, nodeYNum, nodeXNum, nodeYNum);
			connX = new Connection(localXPort);
			connY = new Connection(localYPort);
			System.out.printf("[%d,%d]Attempting connection to coordinator\n", nodeXNum, nodeYNum, nodeXNum, nodeYNum);
			DataIO dio = connX.connectIO(coorIP, coorPort);
			dosCoor = dio.getDos();
			System.out.printf("[%d,%d]Sending self configuration to coordinator\n", nodeXNum, nodeYNum);
			dosCoor.writeInt(nodeXNum);
			dosCoor.writeInt(nodeYNum);
			dosCoor.writeUTF(LocalIp);
			dosCoor.writeInt(localXPort);
			dosCoor.writeInt(localYPort);
			disCoor = dio.getDis();
			System.out.printf("[%d,%d]Reading configuration info from coordinator\n", nodeXNum, nodeYNum, nodeXNum,
					nodeYNum);
			n = disCoor.readInt(); // read total matrix dimension
			m = disCoor.readInt(); // get my matrix dimension from coordinator
			if (nodeXNum >= (n / m))
				throw new Error("Worker X id must be less than n/m ");
			if (nodeYNum >= (n / m))
				throw new Error("Worker Y id must be less than n/m ");
			a = new int[m][m];
			b = new int[m][m];
			c = new int[m][m];
			System.out.println("Reached here");

			String ipLeft = disCoor.readUTF(); // left block connection info
			int portLeft = disCoor.readInt();
			String ipRight = disCoor.readUTF(); // right block connection info
			int portRight = disCoor.readInt();
			String ipTop = disCoor.readUTF();
			int portTop = disCoor.readInt();
			String ipBottom = disCoor.readUTF();
			int portBottom = disCoor.readInt();

			System.out.printf("[%d,%d]Got configuration info from coordinator, attempting pipe line connections\n",
					nodeXNum, nodeYNum);

			if (nodeYNum % 2 == 0) { // Even # worker connecting
				System.out.printf("[%d,%d]Attempting connection to left worker %d:%d \n", nodeXNum, nodeYNum,
						(nodeXNum + (n / m) - 1) % (n / m), nodeYNum);
				dosLeft = connX.connect2write(ipLeft, portLeft);
				System.out.printf("[%d,%d]Connected to left worker\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Waiting for connection from right worker with id [%d,%d]\n", nodeXNum,
						nodeYNum, (nodeXNum + 1) % (n / m), nodeYNum);
				disRight = connX.accept2read();
				System.out.printf("[%d,%d]Connected to right worker\n", nodeXNum, nodeYNum);
			} else { // Odd # worker connecting 
				System.out.printf("[%d,%d]Waiting for connection from right worker with id [%d,%d]\n", nodeXNum,
						nodeYNum, (nodeXNum + 1) % (n / m), nodeYNum);
				disRight = connX.accept2read();
				System.out.printf("[%d,%d]Connected to right worker\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Attempting connection to left worker %d:%d \n", nodeXNum, nodeYNum,
						(nodeXNum + (n / m) - 1) % (n / m), nodeYNum);
				dosLeft = connX.connect2write(ipLeft, portLeft);
				System.out.printf("[%d,%d]Connected to left worker\n", nodeXNum, nodeYNum);
			}


			if (nodeXNum % 2 == 0) {
				System.out.printf("[%d,%d]Attempting connection to top worker %d:%d \n", nodeXNum, nodeYNum, nodeXNum,
						(nodeYNum + (n / m) - 1) % (n / m));
				dosTop = connY.connect2write(ipTop, portTop);
				System.out.printf("[%d,%d]Connected to top worker\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Waiting for connection from bottom worker with id [%d,%d]\n", nodeXNum,
						nodeYNum, (nodeXNum), (nodeYNum + 1) % (n / m));
				disBottom = connY.accept2read();
				System.out.printf("[%d,%d]Connected to bottom worker\n", nodeXNum, nodeYNum);
			} else {

				System.out.printf("[%d,%d]Waiting for connection from bottom worker with id [%d,%d]\n", nodeXNum,
						nodeYNum, (nodeXNum), (nodeYNum + 1) % (n / m));
				disBottom = connY.accept2read();
				System.out.printf("[%d,%d]Connected to bottom worker\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Attempting connection to top worker %d:%d \n", nodeXNum, nodeYNum, nodeXNum,
						(nodeYNum + (n / m) - 1) % (n / m));
				dosTop = connY.connect2write(ipTop, portTop);
				System.out.printf("[%d,%d]Connected to top worker\n", nodeXNum, nodeYNum);
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new Error("Error configuring");
		}
		System.out.printf("[%d,%d]Configuration Complete.\n", nodeXNum, nodeYNum);
	}

	void compute() {
		int dim = m;
		int width = m;
		// get the initial block from coordinator
		System.out.printf("[%d,%d]GET Matrix A from coordinator\n", nodeXNum, nodeYNum);
		for (int i = 0; i < dim; i++) {
			for (int j = 0; j < width; j++) {
				try {
					a[i][j] = disCoor.readInt();
				} catch (IOException ioe) {
					System.out.println("error: " + i + ", " + j);
					ioe.printStackTrace();
					throw new Error("Error getting matrix A coordinator");
				}
			}
		}
		System.out.printf("[%d,%d]Matrix A received\n", nodeXNum, nodeYNum);
		MatrixMultiple.displayMatrix(a);
		System.out.printf("[%d,%d]GET Matrix B from Coordinator\n", nodeXNum, nodeYNum);
		for (int i = 0; i < dim; i++) {
			for (int j = 0; j < width; j++) {
				try {
					b[i][j] = disCoor.readInt();
				} catch (IOException ioe) {
					System.out.println("error: " + i + ", " + j);
					ioe.printStackTrace();
					throw new Error("Error Getting Matrix B from coordinator");
				}
			}
		}
		System.out.printf("[%d,%d]Matrix B received\n", nodeXNum, nodeYNum);
		MatrixMultiple.displayMatrix(b);

		int iterations = n;
		while (iterations-- > 0) {
			System.out.printf("[%d,%d]Iterations left %d\n", nodeXNum, nodeYNum, iterations);
			// perform computation
			for (int i = 0; i < dim; i++)
				for (int j = 0; j < width; j++) {
					c[i][j] += a[i][j] * b[i][j];
				}
			MatrixMultiple.displayMatrix(c);

			// shift matrix a toward left
			int[] tempIn;
			int[] tempOut;

			if (nodeYNum % 2 == 0) { // Even # worker shifting procedure
				System.out.printf("[%d,%d]send current value to left\n", nodeXNum, nodeYNum);
				for (int i = 0; i < dim; i++) {
					try {
						dosLeft.writeInt(a[i][0]);
					} catch (IOException ioe) {
						System.out.println("error in sending to left, row=" + i);
						ioe.printStackTrace();
						throw new Error("Error sending initial matrix from coordinator");
					}
				}
				System.out.printf("[%d,%d]Current value sent to neighbour\n", nodeXNum, nodeYNum);
				// local shift
				for (int i = 0; i < dim; i++) {
					for (int j = 1; j < width; j++) {
						a[i][j - 1] = a[i][j];
					}
				}
				System.out.printf("[%d,%d]Read from Right neighbour \n", nodeXNum, nodeYNum);
				// receive the rightmost column
				for (int i = 0; i < dim; i++) {
					try {
						a[i][width - 1] = disRight.readInt();
					} catch (IOException ioe) {
						System.out.println("error reading from right, row=" + i);
						ioe.printStackTrace();
					}
				}
				System.out.printf("[%d,%d]Read from Right neighbour success\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Shifted Matrix A\n", nodeXNum, nodeYNum);
				MatrixMultiple.displayMatrix(a);

			} else { // Odd # worker shifting procedure

				tempIn = new int[dim];
				tempOut = new int[dim];
				System.out.printf("[%d,%d]Read from right Neighbour\n", nodeXNum, nodeYNum);
				for (int i = 0; i < dim; i++) { // receive a row from right
					try {
						tempIn[i] = disRight.readInt();
					} catch (IOException ioe) {
						System.out.println("Error receiving from right, row=" + i);
						ioe.printStackTrace();
					}
				}

				for (int i = 0; i < dim; i++) { // local shift
					tempOut[i] = a[i][0];
				}
				for (int i = 0; i < dim; i++) {
					for (int j = 1; j < width; j++) {
						a[i][j - 1] = a[i][j];
					}
				}
				for (int i = 0; i < dim; i++) {
					a[i][width - 1] = tempIn[i];
				}
				System.out.printf("[%d,%d]received data from right Neighbour\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Send Row to left Neignbour\n", nodeXNum, nodeYNum);
				for (int i = 0; i < dim; i++) { // send leftmost column to left
												// node
					try {
						dosLeft.writeInt(tempOut[i]);
					} catch (IOException ioe) {
						System.out.println("error in sending left, row=" + i);
						ioe.printStackTrace();
					}
				}
				System.out.printf("[%d,%d]Current value sent successfully to left neighbour\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Shifted Matrix A\n", nodeXNum, nodeYNum);
				MatrixMultiple.displayMatrix(a);

			}
			if (nodeXNum % 2 == 0) {

				// do the same for b now
				System.out.printf("[%d,%d]Send B to top neighbour\n", nodeXNum, nodeYNum);
				for (int i = 0; i < width; i++) {
					try {
						dosTop.writeInt(b[0][i]);
					} catch (IOException ioe) {
						System.out.println("Error sending to top neighbour, col=" + i);
						ioe.printStackTrace();
					}
				}
				System.out.printf("[%d,%d]Matrix B sent to top neighbour\n", nodeXNum, nodeYNum);
				// local shift
				for (int i = 1; i < dim; i++) {
					for (int j = 0; j < width; j++) {
						b[i - 1][j] = b[i][j];
					}
				}
				// receive the bottom most column
				System.out.printf("[%d,%d]Read next Col from bottom neighbour\n", nodeXNum, nodeYNum);
				for (int i = 0; i < width; i++) {
					try {
						b[dim - 1][i] = disBottom.readInt();
					} catch (IOException ioe) {
						System.out.println("error in receiving from bottom neighbour, col=" + i);
						ioe.printStackTrace();
					}
				}
				System.out.printf("[%d,%d]Read from bottom neighbour success\n", nodeXNum, nodeYNum);
				System.out.printf("[%d,%d]Shifted Matrix B\n", nodeXNum, nodeYNum);
				MatrixMultiple.displayMatrix(b);
			} else {
				// do the same for b now

				tempIn = new int[width];
				tempOut = new int[width];
				// receive the bottom most column
				System.out.printf("[%d,%d]Read col from bottom neighbour \n", nodeXNum, nodeYNum);
				for (int i = 0; i < width; i++) {
					try {
						tempIn[i] = disBottom.readInt();
					} catch (IOException ioe) {
						System.out.println("error in receiving from bottom neighbour, col=" + i);
						ioe.printStackTrace();
					}
				}
				System.out.printf("[%d,%d]Read success from Bottom Neighbour \n", nodeXNum, nodeYNum);

				for (int i = 0; i < width; i++) { // local shift
					tempOut[i] = b[0][i];
				}
				for (int i = 1; i < dim; i++) {
					for (int j = 0; j < width; j++) {
						b[i - 1][j] = b[i][j];
					}
				}
				for (int i = 0; i < width; i++) {
					b[dim - 1][i] = tempIn[i];
				}

				System.out.printf("[%d,%d]Send Col to top neighbour\n", nodeXNum, nodeYNum);
				for (int i = 0; i < width; i++) { // send leftmost column to
													// left node
					try {
						dosTop.writeInt(tempOut[i]);
					} catch (IOException ioe) {
						System.out.println("error in sending to top neighbour, col=" + i);
						ioe.printStackTrace();
					}
				}
				System.out.printf("[%d,%d]Col b sent successfully to top neighbour\n", nodeXNum, nodeYNum);

				System.out.printf("[%d,%d]SHIFTED MATRIX B \n", nodeXNum, nodeYNum);
				MatrixMultiple.displayMatrix(b);

			}

		}
		System.out.printf("[%d,%d]All iterations complete\n", nodeXNum, nodeYNum);
		// now write output to coordinator
		for (int i = 0; i < dim; i++) {
			for (int j = 0; j < width; j++) {
				try {
					dosCoor.writeInt(c[i][j]);
				} 
				catch (IOException e) {
					e.printStackTrace();
					throw new Error("Error sending output");
				}
			}
		}
		MatrixMultiple.displayMatrix(c);

		System.out.printf("[%d,%d]Compute Done\n", nodeXNum, nodeYNum);

	}

	public static void main(String[] args) throws IOException {
		if (args.length != 6) {
			System.out.println("usage: java Worker workerXid workerYid Workerxport workeryport coordinator-ip coordinator-port-num");
			System.exit(1);
		}
		int workerXID = Integer.parseInt(args[0]);
		int workerYID = Integer.parseInt(args[1]);
		int portXNum = Integer.parseInt(args[2]); 
		int portYNum = Integer.parseInt(args[3]);
		 		
		BufferedReader reader = new BufferedReader(new
		InputStreamReader(System.in)); // Get ip address System.out.print(
		System.out.println("Enter your system IP :\n"); 
		String ip = reader.readLine();
		System.out.println(ip);
		String localip = InetAddress.getByName(ip).getHostAddress();
		System.out.println(localip);
		Worker worker = new Worker(workerXID, workerYID, portXNum, portYNum, args[4], Integer.parseInt(args[5]), localip);
		worker.run();				

	}

	@Override
	public void run() {
		this.configurate();
		this.compute();
		System.out.printf("[%d,%d]Done.\n", nodeXNum, nodeYNum);

	}
}
