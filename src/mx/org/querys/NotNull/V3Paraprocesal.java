package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Paraprocesal {

    private static final Logger LOGGER = Logger.getLogger(V3Paraprocesal.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Estatus del expediente no debe ser 9 = No_identificado
    public ArrayList Estatus_expedienteNi(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_EXPEDIENTE = 9 THEN 'No_identificado' " +
              "     ELSE CAST(ESTATUS_EXPEDIENTE AS VARCHAR) END AS ESTATUS_EXPEDIENTE, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, " +
              "PERIODO " +
              "FROM V3_TR_PARAPROCESALJL " +
              "WHERE ESTATUS_EXPEDIENTE = 9";

        //System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("COMENTARIOS")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Estatus_expedienteNi()", ex);
        }

        return Array;
    }

    // INCOMPETENCIA no debe ser 9
    public ArrayList IncompetenciaNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, " +
              "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, " +
              "PERIODO " +
              "FROM V3_TR_PARAPROCESALJL " +
              "WHERE INCOMPETENCIA = 9";

        //System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("COMENTARIOS")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en IncompetenciaNI()", ex);
        }

        return Array;
    }

    // INCOMPETENCIA = 1 y ya tiene datos posteriores (no debería)
    public ArrayList PivIncompetencia(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, PERIODO " +
              "FROM V3_TR_PARAPROCESALJL " +
              "WHERE INCOMPETENCIA = 1 AND (" +
              "FECHA_PRESENTA_SOLI IS NOT NULL OR FECHA_ADMISION_SOLI IS NOT NULL OR PROMOVENTE IS NOT NULL OR " +
              "ESPECIFIQUE_PROMOVENTE IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR FECHA_CONCLUSION_EXPE IS NOT NULL" +
              ")";

        //System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en PivIncompetencia()", ex);
        }

        return Array;
    }

    // ESTATUS_EXPEDIENTE = 2 (En proceso de Resolución) y ya tiene FECHA_CONCLUSION_EXPE (no debería)
    public ArrayList Estatus_Expediente(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ESTATUS_EXPEDIENTE = 2 THEN 'En proceso de Resolucion' " +
              "     ELSE CAST(ESTATUS_EXPEDIENTE AS VARCHAR) END AS ESTATUS_EXPEDIENTE, " +
              "PERIODO " +
              "FROM V3_TR_PARAPROCESALJL " +
              "WHERE ESTATUS_EXPEDIENTE = 2 AND FECHA_CONCLUSION_EXPE IS NOT NULL";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Estatus_Expediente()", ex);
        }

        return Array;
    }
}
