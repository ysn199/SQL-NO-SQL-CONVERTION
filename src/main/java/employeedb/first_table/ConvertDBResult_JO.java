/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package employeedb.first_table;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileWriter;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author yassi
 */
public class ConvertDBResult_JO {

    public static void main(String[] args) throws SQLException, IOException {
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3308/employee", "root", "");

        Statement stm = con.createStatement();

        String q = "SELECT * FROM EMP,DEPT WHERE emp.DEPTNO=dept.DEPTNO";
        ResultSet rs = stm.executeQuery(q);

        JSONObject jSONObject = new JSONObject();
        JSONArray jSONArray = new JSONArray();

        while (rs.next()) {
            JSONObject result = new JSONObject();
            result.put("empno", rs.getInt("empno"));
            result.put("name", rs.getString("ename"));
            result.put("job", rs.getString("JOB"));
            result.put("mgr", rs.getInt("MGR"));
            result.put("embauche", rs.getDate("embauche"));
            result.put("sal", rs.getInt("sal"));
            result.put("comm", rs.getInt("comm"));
            result.put("deptno", rs.getInt("deptno"));
            result.put("dname", rs.getString("dname"));
            result.put("loc", rs.getString("loc"));
            jSONArray.add(result);
            //System.out.println(empno + " " + name + " " + job + " " + mgr + " " + embauche + " " + sal + " " + comm + " " + deptno + " " + dname + " " + loc);
        }

        jSONObject.put("employeeinfo", jSONArray);
        try (FileWriter file = new FileWriter("C:\\Users\\yassi\\Documents\\NetBeansProjects\\first_table\\employeeinfo.json")) {
            file.write(jSONObject.toJSONString());
        }

        con.close();

        System.out.println("Done!!");
    }
}
