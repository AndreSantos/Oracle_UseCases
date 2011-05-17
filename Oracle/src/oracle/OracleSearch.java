package oracle;


import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.jdbc.driver.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

class OracleReqThread extends Thread {
        // Database definition
    static Connection conn = null;
    static String db = "ssnteit0";
    static String drivers = "jdbc:oracle:thin:";
    static String host = "localhost"; //"10.112.27.174";
    static String port = "1521";
    static String sid = "ssnteit0";
    static String user = "SOLR";
    static String password = "SOLR";
    static String database = "SOLR_DB";

    
    private int ops_sec;
    static int secs, fieldlength;
    static int textfields, intfields;
    static int indexed, results;
    static int ind = 0;
    static int ops = 0;
    private int id;


    static int nwords;
    private int [] docs;
    private int [] time;

    static int total, mode;
    static String [] palavras;

    public static String getDataBase() {
        db = drivers + "@" + "(DESCRIPTION =(ADDRESS_LIST = (ADDRESS = (PROTOCOL=TCP)(HOST=" + host + ")(PORT=" + port + ")))(CONNECT_DATA =(SERVICE_NAME = " + sid + ")))";
        return db;
    }

    public static Connection connect() throws Exception {
        Class.forName ("oracle.jdbc.driver.OracleDriver");
        conn = DriverManager.getConnection(OracleInsert.getDataBase(), user, password);
        conn.setAutoCommit(false);
        return conn;
    }

    public static Statement getStatement() throws Exception {
        Statement stmt = null;
        if (conn == null)
            conn = connect();

        stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return stmt;
    }

    public static CallableStatement getOracleCallableStatement(String sqlString) throws Exception {
        CallableStatement cstmt = null;
        if (conn == null)
            conn = connect();
        cstmt = (CallableStatement)conn.prepareCall(sqlString);
        return cstmt;
    }

    public static PreparedStatement getOraclePreparedStatement(String sqlString) throws Exception {
        PreparedStatement cstmt = null;
        if (conn == null)
            conn = connect();
        cstmt = conn.prepareStatement(sqlString);
        return cstmt;
    }

    public static Connection getConnection() {
        return conn;
    }

    public OracleReqThread(int i, int ops) {
        id = i;
        docs = new int[1000];
        time = new int[1000];
        ops_sec = ops;
    }

    public void run() {
        for (int k = 0;k < secs; k++) {
            long tempo = 0;
            for (int l = 0;l < ops_sec; l++) {
                if (tempo > 1050)
                    break;
                String query = build_query();
                try {
                    long t = getResponse(k, query);
                    tempo += t;
                    //if (t <= 1) System.out.println(query);
                } catch (Exception ex) {
                    Logger.getLogger(OracleReqThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (tempo > 1050) {
                //System.out.println("CUT: " + (k+1) + " (Thread " + id + ": " + tempo + " > 1s)");
                time[k] = -10000000;
            }
            while (tempo < 950) {
                long start = System.currentTimeMillis();
                try {
                    sleep( (int)((1000 - tempo) * 0.8) );
                } catch (InterruptedException ex) {
                    Logger.getLogger(OracleReqThread.class.getName()).log(Level.SEVERE, null, ex);
                }
                long end = System.currentTimeMillis();
                tempo += end - start;
            }
        }
    }

    private synchronized long getResponse(int s, String query) throws SQLException, Exception {
        long start = System.currentTimeMillis();
        CallableStatement cstmt = getOracleCallableStatement(query);
        ResultSet rs = cstmt.executeQuery();
        int i = 0;
        while ( rs.next() )
            i++;
        rs.close();
        cstmt.close();
        long end   = System.currentTimeMillis();
        time[s] += end - start;
        docs[s] += i;
        if (s > 0)
            ops++;
        return end - start;
    }
    
    private String build_query() {
        String word = tune_string_by_size();
        String s = "SELECT * FROM " + database + " WHERE (";
        for (int k = 1; k <= indexed; k++) {
            s += name(k-1) + "_TEXT = '" + word + "'";
            if (k != indexed)
                s += " OR ";
        }
        s += ") AND rownum <= " + String.valueOf(results);
        return s;
    }

    private static String name(int l) {
        String s = "";
        if (l > 23 * 23) {
            s = String.valueOf((char) ('A' + (l % 23)));
            l /= 23;
        }
        return String.valueOf((char) ('A' + (l / 23))) + String.valueOf((char) ('A' + (l % 23))) + s;
    }

    private synchronized int change_index() {
        ind = (ind + 1) % total;
        return ind;
    }
    private String tune_string_by_size() {
        int index = change_index();
        int len = palavras[index].length();
        String word = palavras[index];
        while (len <= fieldlength) {
            word += "-" + palavras[index];
            len  += 1   + palavras[index].length();
        }
        return word.substring(0, fieldlength);
    }


    public int getTime(int t) {
        return time[t];
    }
    public int getDocs(int t) {
        return docs[t];
    }
}

public class OracleSearch
{
    // Database definition
    static Connection conn = null;
    static String db = "ssnteit0";
    static String drivers = "jdbc:oracle:thin:";
    static String host = "localhost"; //"10.112.27.174";
    static String port = "1521";
    static String sid = "ssnteit0";
    static String user = "SOLR";
    static String password = "SOLR";
    static String database = "SOLRB";


    static int total = 0;
    static float tam_medio = 0;
    static String [] palavras = new String[500000];

    static String dictionary_path, index_path;
    static int textfields, intfields;
    static int indexed;
    static int fieldlength;
    static int variable, num_threads, ops_per_sec, secs, nwords;
    static int results;

    public static void lerPalavras() {
        File file = new File(dictionary_path);
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        DataInputStream dis = null;

        try {
            fis = new FileInputStream(file);

            // Here BufferedInputStream is added for fast reading.
            bis = new BufferedInputStream(fis);
            dis = new DataInputStream(bis);

            while (dis.available() != 0) {
                palavras[total++] = dis.readLine();
                tam_medio += palavras[total-1].length();
            }
            tam_medio /= total;

            // dispose all the resources after using them.
            fis.close();
            bis.close();
            dis.close();
        }catch (FileNotFoundException e) {
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static int searchdata() throws Exception {
        OracleReqThread.secs        = secs;
        OracleReqThread.nwords      = nwords;
        
        OracleReqThread.palavras    = palavras;
        OracleReqThread.total       = total;
        OracleReqThread.fieldlength = fieldlength;
        
        OracleReqThread.textfields = textfields;
        OracleReqThread.intfields  = intfields;
        OracleReqThread.indexed    = indexed;
        OracleReqThread.results    = results;

        OracleReqThread.palavras  = palavras;
        OracleReqThread.total     = total;
        OracleReqThread.ops       = 0;
        OracleReqThread.ind       = 0;

        
        int operations = ops_per_sec;
        ArrayList<OracleReqThread> threads = new ArrayList<OracleReqThread>();
        for (int k = 0;k < num_threads; k++) {
            int ops_thread = operations / (num_threads - k);
            threads.add(new OracleReqThread(k, ops_thread));
            operations -= ops_thread;
        }
        for (int k = 0;k < num_threads; k++)
            threads.get(k).start();
        for (int k = 0;k < num_threads; k++)
            threads.get(k).join();

        float [] docs = new float[10000];
        float [] time = new float[10000];
        for (int k = 1;k < secs; k++) {
            docs[k] = 0;
            time[k] = 0;
            for (int l = 0;l < num_threads; l++) {
                if (threads.get(l).getTime(k) < 0) {
                    time[k] = -10000000.0f;
                    break;
                }
                else {
                    time[k] += threads.get(l).getTime(k);
                    docs[k] += threads.get(l).getDocs(k);
                }
            }
            //System.out.println(time[k] + " " + k + ": " + threads.get(0).getTime(k) + " , " +  threads.get(1).getTime(k) + " , " +  threads.get(2).getTime(k));
            //System.out.println(time[k] + "   d: " + docs[k]);
            time[k] /= ops_per_sec;
            docs[k] /= ops_per_sec;
        }



        float arr[]  = new float[time.length];
        float arr2[] = new float[time.length];

        int i = 0;
        for (int k = 0;k < secs; k++)
            if (time[k] > (float) 0.0) {
                arr[i]    = time[k];
                arr2[i++] = docs[k];
            }
        int off = 2;
        if (i < off + off + off)
            return 0;

        for (int k = 0;k < i; k++)
            for (int l = k + 1;l < i; l++)
                if (arr[k] > arr[l]) {
                    float a = arr[k];
                    arr[k] = arr[l];
                    arr[l] = a;
                    float b = arr2[k];
                    arr2[k] = arr2[l];
                    arr2[l] = b;
                }
        float m2 = arr[0] + arr[1] + arr[i - 1] + arr[i - 2];
        float m = (float) 0.0;
        float t = (float) 0.0;
        int s = 0;
        for (int k = off;k < i - off; k++) {
            m += arr[k];
            t += arr2[k];
            m2 += arr[k];
            s++;
        }
        m /= s;
        t /= s;
        if (s == 0) {
            System.out.print(' ');
            return 0;
        }
        else {
            if (variable == 2)
                System.out.print(ops_per_sec + ";");
            if (variable == 3)
                System.out.print(results + ";");
            System.out.print(s + ";" + m + ";" + t + ";" + OracleReqThread.ops);
            for (int k = 0;k < i; k++) {
                System.out.printf("%.1f;",arr[k]);
            }
            System.out.printf("%.2f\n",m2 / i);
        }
        return 1;
    }
    
    
    // ============================== START FUNCTIONS ==================================
    public void search_seconds(int text,int scenario) throws Exception {
        variable    = 0;
        textfields  = text;

        switch (scenario) {
            case 1:  indexed = 1;           break;
            case 2:  indexed = text / 2;    break;
            default: indexed = text;
        }

        int times = 0;
        while (searchdata() == 0 && times <= 10) {
            times++;
        }
        if (times > 10)
            System.out.println("-1;-1");
    }

    public void search_indexed(int ind) throws Exception {
        variable    = 1;
        textfields  = Parameters.SEARCH_MAXTEXTFIELDS;
        indexed     = ind;

        int times = 0;
        while (searchdata() == 0 && times <= 10) {
            times++;
        }
        if (times > 10)
            System.out.println("-1;-1");
    }

    public void search_opspersec(int b, int e) throws InterruptedException, Exception {
        variable        = 2;

        int s = secs;
        for (int k=b;k <= e; k += b) {
            ops_per_sec = k;
            
            if (searchdata() == 0)
                k -= b;

            if (k == 200)
                b = 50;
            
            Thread.sleep(Parameters.REST);
        }
    }
    
    public void search_results(int res) throws InterruptedException, IOException, Exception {
        variable        = 3;

        int b = 10;
        for (int k=b;k <= res; k += b) {
            results = k;
            if (searchdata() == 0)
                k -= b;
            
            Thread.sleep(Parameters.REST);
        }
    }

    
    public OracleSearch(int ind) throws Exception {
        // Parametros
        nwords      = Parameters.WORDS;

        textfields  = Parameters.TEXT;
        intfields   = Parameters.INT;
        fieldlength = Parameters.FIELDLENGTH;
        
        results     = Parameters.RESULTS;
        secs        = Parameters.SECONDS;
        ops_per_sec = Parameters.OPS;
        num_threads = Parameters.THREADS;

        indexed     = ind;

        // Dicionario
        dictionary_path = "dics/dic.txt";
        lerPalavras();
    }
}

// Saber que tabelas existem:
    // Select table_name from tabs;