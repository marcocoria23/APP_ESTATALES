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

        sql = "SELECT TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, REPLACE(MENSAJE,',','') AS MENSAJE " +
              "FROM ERRORES_INSERT";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Array.add(new String[]{
                        rs.getString("TABLA_DESTINO"),
                        rs.getString("CLAVE_ORGANO"),
                        rs.getString("EXPEDIENTE_CLAVE"),
                        rs.getString("ID"),
                        rs.getString("MENSAJE")
                    });
                }
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en TErroresInserTRInicio()", ex);
        }

        return Array;
    }
    
    
     public String Total_Reg_insertadosTR(Connection con,String Tabla) {
      String Valor="";
        sql = "SELECT Count(*) cuenta_reg " +
              "FROM "+Tabla+" ";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                if(rs.next()) {
                        Valor=rs.getString("cuenta_reg");
                        //NOTA: MODIFICAR CLASE PRINCIPAL
                }
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en TErroresInserTRInicio()", ex);
        }

        return Valor;
    }
     
      public String Total_Reg_NITR(Connection con,String Tabla) {
      String Valor="";
        sql = "SELECT Count(*) cuenta_reg " +
              "FROM ERRORES_INSERT WHERE TABLA_DESTINO='"+Tabla+"' ";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                if(rs.next()) {
                        Valor=rs.getString("cuenta_reg");
                        //NOTA: MODIFICAR CLASE PRINCIPAL
                }
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en TErroresInserTRInicio()", ex);
        }

        return Valor;
    }
    
    

   
}
