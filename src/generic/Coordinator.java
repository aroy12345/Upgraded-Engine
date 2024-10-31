package generic;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.Iterator;

import webserver.Server;

public class Coordinator {
    private static final Map<String, WorkerObj> workers = new HashMap<>();

    public static Vector<String> getWorkers() {
        Vector<String> ret = new Vector<>();
        for (String x : workers.keySet()) {
            ret.add(workers.get(x).ip + ":" + Integer.toString(workers.get(x).port));
        }
        return ret;
    }

    public static String workerTable() {
        int count = 0;
        StringBuilder table = new StringBuilder();
        table.append("<html><body>");
        table.append("<table border='1'><tr><th>Worker ID</th><th>IP:Port</th></tr>");

        for (String workerID : workers.keySet()) {
            WorkerObj worker = workers.get(workerID);
            if (System.currentTimeMillis() <= worker.lastInvoked + 15000) {
                table.append("<tr><td>").append(workerID).append("</td><td>");
                String temp = worker.ip + ":" + worker.port;
                table.append("<a href=\"http://").append(temp).append("\">").append(temp).append("</a>");


                table.append("</td></tr>");
                count++;
            }
        }

        table.append("</table>");
        table.append("<html><body>");

        return table.toString();
    }

    public static void registerRoutes() {
        Server.get("/getWorkers", (req, res) -> {
            StringBuilder workerList = new StringBuilder();
            for (String workerId : workers.keySet()) {
                WorkerObj worker = workers.get(workerId);
                workerList.append(workerId).append(":").append(worker.ip).append(":").append(worker.port).append("\n");
            }
            res.status(200, "OK");
            res.body(workerList.toString());
            return null;
        });

        Server.get("/ping", (req,res) -> {
            if (!req.queryParams().contains("id") || !req.queryParams().contains("port")) {
                res.status(400, "Bad Request");
                res.body("Bad Request");
                return null;
            }

            try {
                workers.put(req.queryParams("id"), new WorkerObj(req.ip(), Integer.parseInt(req.queryParams("port")), System.currentTimeMillis()));
            }
            catch( Exception e) { // bad parse 
                e.printStackTrace();
                res.status(400, "Bad Request");
                res.body("Bad Request");
                return null;
            }
            res.status(200, "OK");
            res.body("OK");
            return null;
        });

        Server.get("/workers", (req,res) -> {
            StringBuilder table = new StringBuilder();
            int count = 0;
            for (String x : workers.keySet()) {
                if(System.currentTimeMillis() <= workers.get(x).lastInvoked + 15000) {
                    table.append(x + "," + (workers.get(x).ip) + ":" + Integer.toString(workers.get(x).port) + "\n");
                    count++;
                }
            }
            res.status(200, "OK");
            res.body(Integer.toString(count) + "\n" + table.toString());
            return null;
        });
    }

    protected static class WorkerObj {
        String ip;
        int port;
        long lastInvoked;

        public WorkerObj(String pIp, int pPort, long pLast) {
            ip = pIp;
            port = pPort;
            lastInvoked = pLast;
        }
    }

}

