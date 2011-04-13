package oracle;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Hashtable;
import oracle.jdbc.driver.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;


public class Oracle
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
    static int mode;   // 0 - Human Readable / 1 - Time, Size

    static String dictionary_path, index_path, configuration_path;
    static int nbatches, registers;
    static int textfields, intfields, floatfields;
    static int indexed, fieldlength;

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
    
    public static String getDataBase() {
        /*
        Hashtable conf = ConfigurationReader.getGroupOfValues("DATABASE");
        host = (String)conf.get("HOST");
        port = (String)conf.get("PORT");
        sid = (String)conf.get("SID");
        user = (String)conf.get("USER");
        password = (String)conf.get("PASSWD");
        db = drivers + "@" + host + ":" + port + ":" + sid;
        */

        db = drivers + "@" + "(DESCRIPTION =(ADDRESS_LIST = (ADDRESS = (PROTOCOL=TCP)(HOST=" + host + ")(PORT=" + port + ")))(CONNECT_DATA =(SERVICE_NAME = " + sid + ")))";

        return db;
    }

    public static Connection connect() throws Exception {
        Class.forName ("oracle.jdbc.driver.OracleDriver");
        conn = DriverManager.getConnection(Oracle.getDataBase(), user, password);
        conn.setAutoCommit(false);
        return conn;
    }

    private static void commit() throws Exception {
        Connection conn = connect();
        conn.commit();
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

    public static String getAttr() {
        String str = "id";
        for (int k = 0; k < textfields; k++) {
            String field = name(k) + "_text";
            str += ", " + field;
        }
        for (int k = 0; k < intfields; k++) {
            String field = name(k) + "_int";
            str += ", " + field;
        }
        for (int k = 0; k < floatfields; k++) {
            String field = name(k) + "_float";
            str += ", " + field;
        }
        return str;
    }
    
    public static int insertdata() throws Exception {
        int total_text_data_size = 0;
        int total_non_text_data_size = 0;
        long total_time = 0;
        long start, end;

        int conta = 1;

        for (int b = 0;b < nbatches; b++) {
            int text_data_size = 0;
            int non_text_data_size = 0;

            String query =  "INSERT INTO " + database + "(" + getAttr() + ") VALUES (?";
            for (int l = 0;l < textfields + intfields + floatfields; l++)
                query += ",?";
            query += ")";
            
            PreparedStatement cstmt = getOracleCallableStatement(query);
            for (int k = 0;k < registers; k++) {
                 cstmt.setInt(1, registers * b + k);

                // Text fields
                    // Indexed & Not Indexed
                for (int l = 1;l <= textfields; l++) {
                    String word = tune_string_by_size();
                    cstmt.setString(l+1, word);
                    text_data_size += fieldlength;
                }

                // Int fields
                for (int l = 1;l <= intfields; l++) {
                    int n = (int) (Math.random() * 9000);
                    n += 1000;
                    cstmt.setInt(l+1+textfields, n);
                    non_text_data_size += String.valueOf(n).length();
                }
                // Float fields
                for (int l = 1;l <= floatfields; l++) {
                    float n = (float) (Math.random() * 1000.0f);
                    n += 1000.0f;
                    cstmt.setFloat(l+1+textfields + intfields, n);
                    non_text_data_size += String.valueOf(n).length();
                }
                start = System.currentTimeMillis();
                cstmt.addBatch();
                end = System.currentTimeMillis();
                total_time += end - start;
            }
            start = System.currentTimeMillis();
            try {
                cstmt.executeBatch();
                conn.commit();
            }
            catch(BatchUpdateException e) {
                conta = 0;
                System.out.println("Erro!");
            }
            finally {
                cstmt.close();
            }
            end = System.currentTimeMillis();
            total_time += end - start;

            total_text_data_size        += text_data_size;
            total_non_text_data_size    += non_text_data_size;
        }
        double oracle_size = measure_size1();
        double oracle_size2 = measure_size2();
        double oracle_size3 = measure_size3();
        String size_s = String.valueOf(oracle_size) + ";" + String.valueOf(oracle_size2) + ";" + String.valueOf(oracle_size3);
        size_s = size_s.replace(".", ",");
        if (mode == 2 && conta == 1)
            System.out.println(registers + ";" + total_time + ";" + (total_text_data_size + total_non_text_data_size) + ";" + size_s);
        return conta;
    }
    
    public void start_insert(int b,int e) throws Exception {
        for (int k = b;k <= e;k += 1000) {
            registers = k;
            if (k != b)
                drop_table();
            create_table();
            if (insertdata() == 0)
                k -= 1000;            
        }
        conn.close();
    }

    public Oracle(String [] args) throws Exception {
        // Parametros
        nbatches    = Integer.parseInt(args[0]);
        registers   = Integer.parseInt(args[1]);
        textfields  = Integer.parseInt(args[2]);
        fieldlength = Integer.parseInt(args[3]);
        intfields   = Integer.parseInt(args[4]);
        floatfields = Integer.parseInt(args[5]);
        indexed     = Integer.parseInt(args[6]);
        mode        = Integer.parseInt(args[8]);

        // Dicionario
        dictionary_path = "dics/dic_" + args[7] + ".txt";
        lerPalavras();
    }

    private static double measure_size1() throws Exception {
        String str = "analyze table " + database + " compute statistics";
        CallableStatement cstmt = getOracleCallableStatement(str);
        cstmt.executeQuery();
        cstmt.close();

        String str2 = "select sum(bytes)/1024 \"Bytes Used\" from dba_segments where segment_name like '%" + database + "%'";
        //String str2 = "SELECT num_rows * avg_row_len \"Bytes Used\" from tabs where table_name = '" + database + "'";
        cstmt = getOracleCallableStatement(str2);
        ResultSet rs = cstmt.executeQuery();
        rs.next();
        return rs.getDouble("Bytes used");
    }

    private static double measure_size2() throws Exception {
        String str = "SELECT blocks*8 \"Bytes Used\" from dba_tables where table_name = '" + database + "'";
        CallableStatement cstmt = getOracleCallableStatement(str);
        ResultSet rs = cstmt.executeQuery();
        rs.next();
        double s = rs.getDouble("Bytes used");
        rs.close();
        cstmt.close();
        
        return s;
    }
    
    private static double measure_size3() throws Exception {
        String str = "select blocks*8 \"Used Blocks\" , (blocks + empty_blocks)*8 \"Allocated Blocks\" from dba_tables where table_name = '" + database + "'";
        CallableStatement cstmt = getOracleCallableStatement(str);
        ResultSet rs = cstmt.executeQuery();
        rs.next();
        double s = rs.getDouble("Allocated Blocks");
        rs.close();
        cstmt.close();
        return s;
    }

    public static void drop_table() throws Exception {
        String str = "DROP TABLE " + database;
        CallableStatement cstmt = getOracleCallableStatement(str);
        cstmt.execute();
        cstmt.close();
        conn.commit();
    }

    private static void create_table() throws Exception {
        String str = "CREATE TABLE " + database + "(";
        str += "id      number    primary key";

        for (int k = 0; k < textfields; k++) {
            String field = name(k) + "_text";
            str += ", " + field + "     varchar2(30)";
        }
        for (int k = 0; k < intfields; k++) {
            String field = name(k) + "_int";
            // int t = (int)(Math.random() * 9);
            str += ", " + field + "     int";
        }
        for (int k = 0; k < floatfields; k++) {
            String field = name(k) + "_float";
           //int t1 = (int)(Math.random() * 6), t2 = (int)(Math.random() * 5);
            str += ", " + field + "     float";
        }
        str += ")";
        CallableStatement cstmt = getOracleCallableStatement(str);
        cstmt.execute();
        cstmt.close();
        conn.commit();
        for (int k = 0; k < indexed; k++) {
            str = "CREATE INDEX " + name(k) + "_idx_" + database + " ON " + database + "(";
            str += name(k) + "_text)";
            cstmt = getOracleCallableStatement(str);
            cstmt.execute();
            cstmt.close();
            conn.commit();
        }
    }
    
    private static String name(int l) {
        return String.valueOf((char) ('a' + (l / 23))) + String.valueOf((char) ('a' + (l % 23)));
    }

    private static String tune_string_by_size() {
        String word = "";
        int len = 0;
        while (len < fieldlength) {
            int rand = (int) ( Math.random() * total );
            if (len > 0)
                word += " ";
            word += palavras[rand];
            len += palavras[rand].length();
        }
        return word.substring(0, fieldlength);
    }


    private static boolean has_content() throws SQLException, Exception {
        String str = "SELECT count(*) as tot FROM " + database;
        CallableStatement cstmt = getOracleCallableStatement(str);
        ResultSet rs = cstmt.executeQuery();
        rs.next();
        int val = rs.getInt("tot");

        rs.close();
        cstmt.close();

        return val > 0;
    }

    private static void delete_contents() throws Exception {
        if (mode != 2)
            System.out.println("Checking previous contents...");
        while (has_content()) {
            if (mode != 2)
                System.out.println("Preparing to delete contents...");
            String str = "DELETE " + database;
            Statement stmt = getStatement();
            stmt.executeUpdate(str);
            if (mode != 2)
                System.out.println("Content deleted.");
            stmt.close();
        }
        conn.commit();
    }
}

// Saber que tabelas existem:
    // Select table_name from tabs;