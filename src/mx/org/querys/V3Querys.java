package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Querys {

    private static final Logger LOG = Logger.getLogger(V3Querys.class.getName());

    String sql;
    ArrayList<String[]> Array;


    //se usa
    public ArrayList TErroresInserTRInicio(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT TABLA, CLAVE_ORGANO, CLAVE_EXPEDIENTE, ID, REPLACE(EXCEPCION,',','') AS EXCEPCION, " +
              "       USUARIO, FECHA_HORA " +
              "FROM V3_ERRORES_INSERT_RALABTR ";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Array.add(new String[]{
                        rs.getString("TABLA"),
                        rs.getString("CLAVE_ORGANO"),
                        rs.getString("CLAVE_EXPEDIENTE"),
                        rs.getString("ID"),
                        rs.getString("EXCEPCION"),
                        rs.getString("USUARIO"),
                        rs.getString("FECHA_HORA"),
                        //NOTA: MODIFICAR CLASE PRINCIPAL
                    });
                }
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en TErroresInserTRInicio()", ex);
        }

        return Array;
    }
    
    
     public ArrayList Total_Reg_insertadosTR(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT TABLA_DESTINO, CLAVE_ORGANO, CLAVE_EXPEDIENTE, ID, MENSAJE, " +
              "       USUARIO, FECHA_HORA " +
              "FROM V3_ERRORES_INSERT_RALABTR ";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Array.add(new String[]{
                        rs.getString("TABLA"),
                        rs.getString("CLAVE_ORGANO"),
                        rs.getString("CLAVE_EXPEDIENTE"),
                        rs.getString("ID"),
                        rs.getString("EXCEPCION"),
                        rs.getString("USUARIO"),
                        rs.getString("FECHA_HORA"),
                        //NOTA: MODIFICAR CLASE PRINCIPAL
                    });
                }
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en TErroresInserTRInicio()", ex);
        }

        return Array;
    }
    
    

   
}
