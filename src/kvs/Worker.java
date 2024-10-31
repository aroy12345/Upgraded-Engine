package kvs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.PrintWriter;


import tools.KeyEncoder;
import webserver.Server;

import static webserver.Server.*;

public class Worker extends generic.Worker {
	private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> data = new ConcurrentHashMap<>();
	private static Random random;
	private static String storageDir;
	
    static synchronized Row getRow(String table, String row) throws Exception {
        if (table.startsWith("pt-")) {
           File f = new File(storageDir + File.separator + KeyEncoder.encode(table) + File.separator + KeyEncoder.encode(row));
           if (!f.exists()) {
              return null;
           } else {
              FileInputStream in = new FileInputStream(f);
              Row r = Row.readFrom((InputStream)in);
              return r;
           }
        } else {
           ConcurrentHashMap<String, Row> m = data.get(table);
           if (m == null) {
              return null;
           } else {
              Row r = (Row)m.get(row);
              if (r == null) {
                 return null;
              } else {
                 return r;
              }
           }
        }
     }

     static void putRow(String table, Row row) throws IOException {
        if (table.startsWith("pt-")) {
            String tDirectory = storageDir + "/" + table;
            File dir = new File(tDirectory);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File rFile = new File(tDirectory + "/" + KeyEncoder.encode(row.key()));
            if(!rFile.exists()) {
                if(!rFile.createNewFile()) {
                }
            }
            FileOutputStream out = new FileOutputStream(rFile.getPath());
            out.write(row.toByteArray());
            if (!data.containsKey(table)) {
                data.put(table, new ConcurrentHashMap<>());
            }
            data.get(table).put(row.key(), row);
            
            out.close();
        } else {
            if (!data.containsKey(table)) {
                data.put(table, new ConcurrentHashMap<>());
            }
            data.get(table).put(row.key(), row);
        }
    }
    public static void main(String[] args) {
    	random = new Random();
        if (args.length != 3) {
            System.err.println("Error: Usage");
            System.exit(1);
        }

        
        port(Integer.parseInt(args[0]));
        
        storageDir = args[1];
        
        put("/data/:tableName/:row/:column", (req, res) -> {
            String tableName = req.params("tableName");
            String row = req.params("row");
            String column = req.params("column");
            
            res.status(200, "OK");
            res.header("Content-Length", "2");
            res.body("OK");
            
            if(tableName.startsWith("pt-")) {
                        
	            File tableDir = new File(storageDir + "/" + tableName);
	            if (!tableDir.exists()) {
	                tableDir.mkdir();
	            }
	
	            String filename = KeyEncoder.encode(row);
	            try (FileOutputStream fos = new FileOutputStream(storageDir + "/" +  tableName + "/" + filename)) {
	            	Row r = new Row(row);
	            	r.put(column, req.body());
	                byte[] rowData = r.toByteArray();
	                fos.write(rowData);
	            } catch (IOException e) {
	                e.printStackTrace();
	                res.status(500, "Internal Server Error");
	                return null;
	            }
	            return null;
            }
	            
            if (!data.containsKey(tableName)) {
                data.put(tableName, new ConcurrentHashMap<String, Row>());
            }

            if(!data.get(tableName).containsKey(row)) {
            	data.get(tableName).put(row, new Row(row));
            }
            
            data.get(tableName).get(row).put(column, req.bodyAsBytes());
            return null;
        });

        post("/data/:tableName/:row/:column", (req, res) -> {
            String tableName = req.params("tableName");
            String row = req.params("row");
            String column = req.params("column");
            
            res.status(200, "OK");
            res.header("Content-Length", "2");
            res.body("OK");
            
            if(tableName.startsWith("pt-")) {
                        
	            File tableDir = new File(storageDir + "/" + tableName);
	            if (!tableDir.exists()) {
	                tableDir.mkdir();
	            }
	
	            String filename = KeyEncoder.encode(row);
	            try (FileOutputStream fos = new FileOutputStream(storageDir + "/" +  tableName + "/" + filename)) {
	            	Row r = new Row(row);
	            	r.put(column, req.body());
	                byte[] rowData = r.toByteArray();
	                fos.write(rowData);
	            } catch (IOException e) {
	                e.printStackTrace();
	                res.status(500, "Internal Server Error");
	                return null;
	            }
	            return null;
            }
	            
            if (!data.containsKey(tableName)) {
                data.put(tableName, new ConcurrentHashMap<String, Row>());
            }

            if(!data.get(tableName).containsKey(row)) {
            	data.get(tableName).put(row, new Row(row));
            }
            
            data.get(tableName).get(row).put(column, req.bodyAsBytes());
            return null;
        });
        
        
        put("/data/:tableName", (req, res) -> {
            String tableName = req.params("tableName");

            res.status(200, "OK");
            res.header("Content-Length", "2");
            res.body("OK");
            
            if(tableName.startsWith("pt-")) {        
	            File tableDir = new File(storageDir + "/" + tableName);
	            if (!tableDir.exists()) {
	                tableDir.mkdir();
	            }
            }
	
            try {
                ConcurrentHashMap<String, Row> m= data.get(tableName);
                ByteArrayInputStream in = new ByteArrayInputStream(req.bodyAsBytes());
                //batched put
//                while(true) {
//                    Row r = Row.readFrom((InputStream)in);
//                    if (r == null) {
//                        return "OK";
//                    }
//
//                    putRow(tableName, r);
//                }

                Row r = Row.readFrom(in);

               //System.out.println("putting " +  r.key() + " to " + tableName);
                putRow(tableName, r);          
            
            } catch(Exception e) {
                e.printStackTrace();
                return null;
            }
            return null;
        });

        post("/data/:tableName/:row/:column", (req, res) -> {
            String tableName = req.params("tableName");
            String row = req.params("row");
            String column = req.params("column");
        
            if(tableName.startsWith("pt-")) {
                File tableDir = new File(storageDir + "/" + tableName);
                if (!tableDir.exists()) {
                    tableDir.mkdir();
                }
        
                String filename = KeyEncoder.encode(row);
                try (FileOutputStream fos = new FileOutputStream(storageDir + "/" +  tableName + "/" + filename)) {
                    Row r = new Row(row);
                    r.put(column, req.body());
                    byte[] rowData = r.toByteArray();
                    fos.write(rowData);
                } catch (IOException e) {
                    e.printStackTrace();
                    res.status(500, "Internal Server Error");
                    return "Internal Server Error";
                }
                res.status(200, "OK");
            }
        
            if (!data.containsKey(tableName)) {
                data.put(tableName, new ConcurrentHashMap<String, Row>());
            }
        
            if(!data.get(tableName).containsKey(row)) {
                data.get(tableName).put(row, new Row(row));
            }
        
            Row existingRow = data.get(tableName).get(row);
            if(existingRow == null) {
                existingRow = new Row(row);
                data.get(tableName).put(row, existingRow);
            }
            
            existingRow.put(column, req.bodyAsBytes());
            
            res.status(200, "OK");
            return null;
        });
        
        
        get("/data/:tableName/:row/:column", (req, res) -> {
            String tableName = req.params("tableName");
            String row = req.params("row");
            String column = req.params("column");
            
            if(tableName.startsWith("pt-")) { //read from pertistent
            	String filePath = storageDir + "/" + tableName + "/" + row;
                //System.out.println(filePath);

            	File file = new File(filePath);
                if (!file.exists()) {
                	res.status(404, "Not Found");
                    res.body("Not Found");
                    res.header("Content-Length", "8");
                	return null;
                }
                
            	
                try {
                    FileInputStream inputStream = new FileInputStream(file);
                    Row r = Row.readFrom(inputStream);
                    if(!r.values.containsKey(column)) {
                    	res.status(404, "Not Found");
                    	return res;
                    }
                    res.body(r.get(column));
                    inputStream.close();
                    res.status(200, "OK");
                } catch (IOException e) {
                    e.printStackTrace();
                    res.status(500, "Internal Server Error");
                }
           }
           else {
        	   if (!data.containsKey(tableName) || !data.get(tableName).containsKey(row) || !data.get(tableName).get(row).columns().contains(column))  {
                   res.status(404, "Not Found");
                   res.body("Not Found");
                   res.header("Content-Length", "8");
                   return res;
   	            }

               res.body(data.get(tableName).get(row).get(column));
               res.status(200, "OK");
           }
           return null;
        });
        
        get("/tables", (req, res) -> {
            StringBuilder tableNames = new StringBuilder();
            for (String tableName : data.keySet()) {
                tableNames.append(tableName).append("\n");
            }

            File workerDir = new File("__worker");
            if (workerDir.exists() && workerDir.isDirectory()) {
                File[] persistentTables = workerDir.listFiles();
                if (persistentTables != null) {
                    for (File table : persistentTables) {
                        if (table.isDirectory() && table.getName().startsWith("pt-")) {
                            tableNames.append(table.getName()).append("\n");
                        }
                    }
                }
            }

            res.header("Content-Type", "text/plain");
            res.body(tableNames.toString());
            return res;
        });
        
        get("/data/:tableName/:row", (req, res) -> {
        	//The workers should support a GET route for /data/XXX/YYY, where XXX is a table
        	//name and YYY is a row key. If table XXX exists and contains a row with key YYY, the worker should
        	//serialize this row using Row.toByteArray() and send it back in the body of the response. If the table
        	//does not exist or does not contain a row with the relevant key, it should return a 404 error code.
        		
        	
        	String tableName = req.params("tableName");
            String row = req.params("row");
                        
            if(tableName.startsWith("pt-")) { //read from pertistent
            	String filePath = storageDir + "/" + tableName + "/" + row;
            	File file = new File(filePath);
                if (!file.exists()) {
                	res.status(404, "Not Found");
                    res.body("Not Found");
                    res.header("Content-Length", "8");
                	return res;
                }
                
                try {
                    FileInputStream inputStream = new FileInputStream(file);
                    Row r = Row.readFrom(inputStream);
                    res.bodyAsBytes(r.toByteArray());
                    inputStream.close();
                    res.status(200, "OK");
                } catch (IOException e) {
                    e.printStackTrace();
                    res.status(500, "Internal Server Error");
                }
           }
           else {
        	   if (!data.containsKey(tableName) || !data.get(tableName).containsKey(row))  {
                   res.body("Not Found");
                   res.header("Content-Length", "8");
                   return res;
   	            }

               res.bodyAsBytes(data.get(tableName).get(row).toByteArray());
               res.status(200, "OK");

           }
           return null;
        });
        
        get("/count/:tableName", (req, res) -> {
            String tableName = req.params("tableName");

            // if(data.containsKey(tableName)) {
            //     System.out.println("Contains Key :  " + tableName);
            // }
            
            if(tableName.startsWith("pt-")) { //read from persistent
            	String filePath = storageDir + "/" + tableName;
            	System.out.println(filePath);
            	File file = new File(filePath);
                if (!file.exists()) {
                	System.out.println("FILE DOES NOT EXIST :(");
                	res.status(404, "Not Found");
                    res.body("Not Found");
                    res.header("Content-Length", "8");
                	return null;
                }
                res.body(Integer.toString(Objects.requireNonNull(file.listFiles()).length));
            }
            else {
            	if(!data.containsKey(tableName)) {
                    System.out.println(tableName + " Doesn't exist");
            		res.status(404, "Not Found");
                    res.body("Not Found");
                    res.header("Content-Length", "8");
                	return res;
            	}
            	res.body(Integer.toString(data.get(tableName).size()));
            }
            res.status(200, "OK");
            return null;
        });
        
        
        put("/rename/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            String newName = req.body().trim();
            
            if (tableName.startsWith("pt-")) {
                String filePath = storageDir + "/" + tableName;
                File file = new File(filePath);
                if (!file.exists()) {
                    res.status(404, "Not Found");
                    res.body("Not Found");
                    return res;
                }
                
                if(!newName.startsWith("pt-")) {
                	//problem not persistent
                	res.status(400, "Bad Request");
                	res.body("Not a persistent table");
                	return null;
                }
                
                if (new File(storageDir + "/" + newName).exists()) {
                    res.status(409, "Conflict");
                    res.body("Table Already Exists");
                    return null;
                }
                
                File newFile = new File("__worker/" + newName);
                if (file.renameTo(newFile)) {
                    res.status(200, "OK");
                    res.body("OK");
                    return null;
                } else {
                    res.status(500, "Internal Server Error");
                    res.body("Internal Server Error");
                    return null;
                }
            } else {
                if (!data.containsKey(tableName)) {
                    res.status(404, "Not Found");
                    res.body("Table Not Found");
                    return null;
                }
                
                if (data.containsKey(newName)) {
                    res.status(409, "Conflict");
                    res.body("Table Already Exists");
                    return null;
                }
                
                if (newName.startsWith("pt-")) {
                    res.status(400, "Bad Request");
                    res.body("New table should not be persistent");
                    return null;
                }
                
                ConcurrentHashMap<String, Row> temp = data.get(tableName);
                data.remove(tableName);
                data.put(newName, temp);

                res.status(200, "OK");
                res.body("OK");
                return null;
            }
        });
        
        put("/delete/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            if (tableName.startsWith("pt-")) {
                String filePath = storageDir + "/" + tableName;
                File file = new File(filePath);
                if (!file.exists()) {
                    res.status(404, "Not Found");
                    res.body("Not Found");
                    return null;
                }
                
                for (File x : file.listFiles()) {
                	x.delete();
                }
                file.delete();
               
            } else {
                if (!data.containsKey(tableName)) {
                    res.status(404, "Not Found");
                    res.body("Table Not Found");
                    return null;
                }
                
                data.remove(tableName);
                res.status(200, "OK");
                res.body("OK");
            }
            return null;
        });
        
       
        get("/data/:tableName", (req, res) -> {
        			
        	String tableName = req.params("tableName");
            
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");
            
            if(tableName.startsWith("pt-")) { //read from pertistent
            	String filePath = storageDir + "/" + tableName;
            	File file = new File(filePath);
                if (!file.exists()) {
                	res.status(404, "Not Found");
                    res.body("Not Found");
                    res.header("Content-Length", "8");
                	return res;
                }
            	
                try {                    
                    //read all files in directory
                	File[] rows = file.listFiles();
                	for (int i = 0; i < rows.length; i++) {
                		FileInputStream inputStream = new FileInputStream(rows[i]);
                		if ((startRow == null || rows[i].getName().compareTo(startRow) >= 0) &&
                                (endRowExclusive == null || rows[i].getName().compareTo(endRowExclusive) < 0)) {
                			Row r = Row.readFrom(inputStream);
                            res.write(r.toByteArray());
                            res.write("\n".getBytes());
                            res.header("content-type", "text/plain");
                        }
                	}
                	res.write("\n".getBytes());
                    res.status(200, "OK");
                } catch (IOException e) {
                    e.printStackTrace();
                    res.status(500, "Internal Server Error");
                }
           }
           else {
        	   if (!data.containsKey(tableName))  {
                   res.status(404, "Not Found");
                   res.body("Not Found");
                   res.header("Content-Length", "8");
                   return null;
   	            }
        	   	// Itreamig
               res.status(200, "OK");
               res.header("content-type", "text/plain");
               for (Row r : data.get(tableName).values()) {
                   if ((startRow == null || r.key().compareTo(startRow) >= 0) &&
                           (endRowExclusive == null || r.key().compareTo(endRowExclusive) < 0)) {
                	   res.write(r.toByteArray());
                	   res.write("\n".getBytes());
                   }
               }
               res.write("\n".getBytes());
           }
           return res;
        });

    //     get("/data/:table/:row", (req, res) -> {
    //      String table = req.params("table");
    //      String row = req.params("row");
    
    //         Row r = getRow(table, row);
    //         if (r != null) {
    //            res.bodyAsBytes(r.toByteArray());
    //         } else {
    //            res.status(404, "Not found");
    //         }
    //         return null;
    //   });
        
        get("/", (req, res) -> {
        	res.status(200, "OK");
        	StringBuilder htmlContent = new StringBuilder();
            htmlContent.append("<!DOCTYPE html>");
            htmlContent.append("<html><head><title>Tables</title></head><body><table>");
            htmlContent.append("<tr><th>Table Name</th><th> Number of Keys</th></tr>");
            
            TreeSet<String> sorted = new TreeSet<>(data.keySet());
            for (String x : sorted) {
                htmlContent.append("<tr><td><a href=\"/view/" + x + "\">" + x + "</a></td>");
                htmlContent.append("<td>" + data.get(x).size() + "</td></tr>");
            }
            htmlContent.append("</table></body></html>");

            res.body(htmlContent.toString());
            return null;
        });
        
        get("/view/:tableName", (req, res) -> {
        	
            String tableName = req.params("tableName");
            if (!data.containsKey(tableName)) {
            	System.out.println("Table Not Found");
                res.status(404, "Not Found");
                res.body("Table is Not Found");
                return null;
            }

            ConcurrentHashMap<String, Row> table = data.get(tableName);
            TreeSet<String> sortedRows = new TreeSet<>(table.keySet());

            try {
            	
            	
                if(req.queryParams("fromRow") != null) {
                	String fromRow = req.queryParams("fromRow");
                	 if (fromRow != null && !fromRow.isEmpty()) {
                         sortedRows = new TreeSet<>(sortedRows.tailSet(fromRow));
                     }
                }          
            
            Set<String> columns = new TreeSet<>();
            for (String row : sortedRows) {
                Row rowData = table.get(row);
                columns.addAll(rowData.columns());
               
            }
            res.status(200, "OK");
            StringBuilder b = new StringBuilder();
            b.append("<!DOCTYPE html>");
            b.append("<html><head><title>Table ").append(tableName).append("</title></head><body><table>");

            b.append("<tr>");
            b.append("<th></th>");
            for (String column : columns) {
                b.append("<th>").append(column).append("</th>");
            }
            b.append("</tr>");

            int rowCount = 0;
            for (String row : sortedRows) {
                if (rowCount >= 10) {
                    b.append("</table><a href=\"/view/").append(tableName);
                    b.append("?fromRow=").append(row).append("\">Next</a>");
                    break;
                }
                Row rowData = table.get(row);
                b.append("<tr>");
                b.append("<td>").append(row).append("</td>");
                for (String column : columns) {
                    String val = rowData.get(column) != null ? rowData.get(column) : "";
                    b.append("<td>").append(val).append("</td>");
                }
                b.append("</tr>");
                rowCount++;
            }
            b.append("</table></body></html>");
            res.body(b.toString());
            }
            catch(Exception e) {
            	e.printStackTrace();
            }

            return null;
        });
        
        
        try {
        File dir = new File(storageDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        }
        
        
        
        catch(Exception e) {
        	 e.printStackTrace();
        }
        String randomStr = genID(storageDir);

        Worker.startPingThread(args[2], randomStr, Integer.parseInt(args[0]));
    }

    private static String genID(String fname) {
        File f = new File(fname + "/id");
        String str = "";

        try {
            if (f.exists()) {
                str = new String(Files.readAllBytes(f.toPath()));
            } else {
                for (int i = 0; i < 5; i++) {
                    str += ((char) (random.nextInt(26) + 'a'));
                }
                if (f.createNewFile()) {
                	Files.write(f.toPath(), str.getBytes());
                }                
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }
}

