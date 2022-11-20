package src.mapreduce;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class messenger {
    /*
    * Class to create a messenger for handling the communication with the mappers and reducers
    * */

    private static final int mapperPort = 8888;
    private static final int reducerPort = 6666;
    private static final String HOST = "127.0.0.1";
    List<String> listTempFilePaths = new ArrayList<>();
    List<String> listOutputFilePaths = new ArrayList<>();
    ServerSocket mapperServer = null;
    ServerSocket reducerServer = null;
    private final AtomicBoolean mapperThreadExit = new AtomicBoolean(false);
    private final AtomicBoolean reducerThreadExit = new AtomicBoolean(false);
    Socket mapperSocket = null;
    Socket reducerSocket = null;
    DataOutputStream reducerOut = null;
    DataOutputStream mapperOut = null;
    DataInputStream reducerIn = null;
    DataInputStream mapperIn = null;

    public void connectMapper(String inputFilePath, Integer no_of_mappers, String UDFmapperClass, String removePunctuations) throws IOException {
        /*
        * Method that maintains the communication with mappers
        * @params:
        * 	inputFilePath: file path of the input file provided by the user
        * 	no_of_mapper: number of mapper/reducers to be started
        * 	UDFmapperPath: class reference path of the UDF Mapper method provided by the user
        *
        * Method hosts a connection for all the mapper processes to connect to.
        * It then sends the params to each mapper.
        * Waits for their response containing the directory of intermediate files
        * Writes the intermediate file path to a shared list
        * */
        mapperServer = new ServerSocket(mapperPort);
        mapperThreadExit.set(true);
        Thread mapperThread = new Thread(() -> {
            while (mapperThreadExit.get()) {
                try {
                    // Setup the socket connection with mappers
                    mapperSocket = mapperServer.accept();
                    mapperOut = new DataOutputStream(mapperSocket.getOutputStream());
                    mapperIn = new DataInputStream(new BufferedInputStream(mapperSocket.getInputStream()));

                    // Send arguments to mapper
                    mapperOut.writeUTF(inputFilePath);
                    mapperOut.writeUTF(UDFmapperClass);
                    mapperOut.writeUTF(String.valueOf(no_of_mappers));
                    mapperOut.writeUTF(removePunctuations);

                    // Maintain a list of all intermediate file paths created by mappers
                    listTempFilePaths.add(mapperIn.readUTF());
                    if (no_of_mappers == listTempFilePaths.size())
                        shutMapperThread();

                } catch (EOFException e) {} catch (IOException e) {
                    e.printStackTrace();
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }
            // System.err.println("Exiting mapper thread");
        });

        mapperThread.setDaemon(true);
        mapperThread.start();
    }

    public void connectReducer(String OutputFilePath, Integer no_of_mappers, String UDFreducerPath) throws IOException {
        /*
         * Method that maintains the communication with reducers
         * @params:
         * 	outputFilePath: file path of the output file provided by the user
         * 	no_of_mapper: number of mapper/reducers to be started
         * 	UDFreducerPath: class reference path of the UDF Reducer method provided by the user
         *
         * Method hosts a connection for all the mapper processes to connect to.
         * It then sends the params to each mapper.
         * Waits for their response containing the directory of intermediate files
         * Writes the intermediate file path to a shared list
         * */
        reducerServer = new ServerSocket(reducerPort);
        reducerThreadExit.set(true);
        Thread reducerThread = new Thread(() -> {
            while (reducerThreadExit.get()) {
                try {
                    // Setup the socket connection with mappers
                    reducerSocket = reducerServer.accept();
                    reducerOut = new DataOutputStream(reducerSocket.getOutputStream());
                    reducerIn = new DataInputStream(new BufferedInputStream(reducerSocket.getInputStream()));

                    // Send arguments to mapper
                    reducerOut.writeUTF(UDFreducerPath);
                    reducerOut.writeUTF(OutputFilePath);

                    // Maintain a list of all intermediate file paths created by mappers
                    listOutputFilePaths.add(reducerIn.readUTF());
                    if (no_of_mappers == listOutputFilePaths.size())
                        shutReducerThread();

                }  catch (EOFException e) {} catch (IOException e) {
                    e.printStackTrace();
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }
        });

        reducerThread.setDaemon(true);
        reducerThread.start();
    }

    public void shutConnections() throws IOException {
        // Shuts data streams, sockets and Servers
        mapperIn.close();
        reducerIn.close();
        mapperOut.close();
        reducerOut.close();
        mapperSocket.close();
        reducerSocket.close();
        shutMapperServer();
        shutReducerServer();
    }

    public void shutMapperThread() {
        mapperThreadExit.set(false);
    }

    public void shutReducerThread() {
        reducerThreadExit.set(false);
    }

    public void shutMapperServer() {
        try {
            mapperServer.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void shutReducerServer() {
        try {
            reducerServer.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public String getTempFilePaths() {
        String output = "";
        for (String s : listTempFilePaths) output += s + " ";
        return output;
    }

    public List<String> getOutputFilePaths() {
        return listOutputFilePaths;
    }

    public static Pair<DataInputStream, DataOutputStream> createDataStream(String workerType) throws IOException {
        Socket socket = null;
        if (workerType.equals("reducer")) {
            socket = new Socket(HOST, reducerPort);
        } else {
            socket = new Socket(HOST, mapperPort);
        }
        DataInputStream inStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
        return new Pair<DataInputStream, DataOutputStream>(inStream, outStream);
    }
}
