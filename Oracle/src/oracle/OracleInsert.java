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


public class OracleInsert
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
    static String database = "SOLR_DB";


    static int total = 0;
    static int ind = 0;
    
    static float tam_medio = 0;
    static String [] palavras = new String[500000];
    static int mode;   // 0 - Human Readable / 1 - Time, Size

    static String dictionary_path, index_path, configuration_path;
    static int nbatches, registers;
    static int textfields, intfields, floatfields;
    static int indexed, fieldlength;
    static int variable;

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
        conn = DriverManager.getConnection(OracleInsert.getDataBase(), user, password);
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
        String query =  "INSERT INTO " + database + "(" + getAttr() + ") VALUES (?";
        for (int l = 0;l < textfields + intfields + floatfields; l++)
            query += ",?";
        query += ")";

        PreparedStatement cstmt = getOracleCallableStatement(query);
        for (int b = 1;b <= nbatches; b++) {
            int conta = 1;

            int text_data_size = 0;
            int non_text_data_size = 0;
            long start, end;
            long total_time = 0;

            cstmt.clearBatch();
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
                System.out.println("Erro: " + e.getMessage());
                Thread.sleep(5000);
            }
            if (conta == 0) {
                if (nbatches == 1)
                    return 0;
                else
                    b--;
                continue;
            }
            end = System.currentTimeMillis();
            total_time += end - start;


            double oracle_size = 0;
            if (b % 10 == 0 || variable > 0)
                oracle_size = measure_size1(b);
            //double oracle_size2 = measure_size2();
            //double oracle_size3 = measure_size3();
            String size_s = String.valueOf(oracle_size); // + ";" + String.valueOf(oracle_size2) + ";" + String.valueOf(oracle_size3);
            size_s = size_s.replace(".", ",");

            if (mode == 2 && variable <= 0)
                System.out.println(b + ";" + total_time + ";" + (text_data_size + non_text_data_size) + ";" + size_s);
            
            if (mode == 2 && variable == 1)
                System.out.println(registers + ";" + total_time + ";" + (text_data_size + non_text_data_size) + ";" + size_s);
            
            if (mode == 2 && variable == 2)
                System.out.println(textfields + ";" + total_time + ";" + (text_data_size + non_text_data_size) + ";" + size_s);

            if (mode == 2 && variable == 3)
                System.out.println(intfields + ";" + total_time + ";" + (text_data_size + non_text_data_size) + ";" + size_s);

            if (mode == 2 && variable == 4)
                System.out.println(indexed + ";" + total_time + ";" + (text_data_size + non_text_data_size) + ";" + size_s);
        }
        cstmt.close();
        return 1;
    }
    public void just_insert_indexed_fields(int ind) throws Exception {
        variable        = -3;
        nbatches   = Parameters.NBATCHES;
        registers  = Parameters.NREGISTERS;
        textfields = Parameters.SEARCH_MAXTEXTFIELDS;
        indexed    = ind;

        create_table();
        insertdata();
        conn.close();
    }
    public void just_insert_text_fields(int text, int scenario) throws Exception {
        variable        = -2;
        nbatches   = Parameters.NBATCHES;
        registers  = Parameters.NREGISTERS;
        textfields = text;

        switch (scenario) {
            case 1:  indexed = 1;           break;
            case 2:  indexed = text / 2;    break;
            default: indexed = text;
        }

        create_table();
        insertdata();
        conn.close();
    }
    public void just_insert(int b, int r) throws Exception {
        variable  = -1;
        nbatches  =  b;
        registers =  r;

        create_table();
        insertdata();
        conn.close();
    }
    public void insert_batches(int e) throws Exception {
        variable = 0;
        nbatches = e;
        registers = 1000;

        create_table();
        insertdata();
        conn.close();
    }
    public void insert_registers(int b,int e,int sce) throws Exception {
        variable = 1;
        /*
        fieldlength = 10;
        textfields = 10;
        switch (sce) {                 // Indexed fields
            case 1:  indexed = 1; break;
            case 2:  indexed = 5; break;
            default: indexed = 10;
        }
        */

        create_table();
        for (int k = b;k <= e;k += 1000) {
            registers = k;
            if (insertdata() == 0)
                k -= 1000;
            delete_contents();
        }
        conn.close();
    }
    public void insert_text_fields(int e,int scenario) throws Exception {
        variable = 2;
        registers = 250;

        int b = 10;
        for (int k = b;k <= e;k += 10) {
            textfields = k;
            switch (scenario) {
                case 1: indexed = 1;    break;
                case 2: indexed = k/2;  break;
                case 3: indexed = k;    break;
            }
            if (k != b)
                drop_table();
            create_table();

            insertdata();
        }
        conn.close();
    }

    public void insert_int_fields(int e,int scenario) throws Exception {
        variable = 3;
        registers = 250;

        int b = 10;
        for (int k = b;k <= e;k += 10) {
            intfields = k;
            if (k != b)
                drop_table();
            create_table();

            insertdata();
        }
        conn.close();
    }
    public void insert_indexed_fields(int e) throws Exception {
        variable = 4;
        registers = 250;
        textfields = 990;

        int b = 10;
        for (int k = b;k <= e;k += 10) {
            indexed = k;
            if (k != b)
                drop_table();
            create_table();

            insertdata();
        }
        conn.close();
    }


    public OracleInsert(String [] args) throws Exception {
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
        //dictionary_path = "dics/dic_" + args[7] + ".txt";
        dictionary_path = "dics/dic.txt";
        lerPalavras();
    }

    private static double measure_size1(int batch) throws Exception {
        if (batch == 1) {
            String str = "analyze table " + database + " compute statistics";
            CallableStatement cstmt = getOracleCallableStatement(str);
            cstmt.executeQuery();
            cstmt.close();
        }

        String str2 = "select sum(bytes)/1024 \"Bytes Used\" from dba_segments where segment_name like '%" + database + "%'";
        //String str2 = "SELECT num_rows * avg_row_len \"Bytes Used\" from tabs where table_name = '" + database + "'";
        CallableStatement cstmt = getOracleCallableStatement(str2);
        ResultSet rs = cstmt.executeQuery();
        rs.next();
        double s = rs.getDouble("Bytes used");
        rs.close();
        cstmt.close();

        return s;
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
        String s = "";
        if (l > 23 * 23) {
            s = String.valueOf((char) ('A' + (l % 23)));
            l /= 23;
        }
        return String.valueOf((char) ('A' + (l / 23))) + String.valueOf((char) ('A' + (l % 23))) + s;
    }

    private static String tune_string_by_size() {
        ind = (ind + 1) % total;
        int len = palavras[ind].length();
        String word =  palavras[ind];
        while (len < fieldlength) {
            word += "-" + palavras[ind];
            len  += 1   + palavras[ind].length();
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

    public boolean exist_table() throws Exception {
        String str = "SELECT count(*) as tot FROM tabs WHERE table_name = '" + database + "'";
        CallableStatement cstmt = getOracleCallableStatement(str);
        ResultSet rs = cstmt.executeQuery();
        rs.next();
        int val = rs.getInt("tot");
        rs.close();
        cstmt.close();
        return val > 0;
    }
}

// Saber que tabelas existem:
    // Select table_name from tabs;