package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Part_Act_individual {

    private static final Logger LOGGER = Logger.getLogger(V3Part_Act_individual.class.getName());

    String sql;
    ArrayList<String[]> Array;

    /// Actor no debe ser No identificado
    public ArrayList ActorNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 99 THEN 'No identificado' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR " +
              "FROM V3_TR_PART_ACT_INDIVIDUALJL " +
              "WHERE ACTOR = 99";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ACTOR")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en ActorNI()", ex);
        }

        return Array;
    }

    /// CUANDO ACTOR = Beneficiario u Otro no debe capturar desde Sexo hasta Jornada
    public ArrayList Actor_otro(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE " +
              "  WHEN ACTOR = 6 THEN 'Beneficiario' " +
              "  WHEN ACTOR = 7 THEN 'Otro' " +
              "  ELSE CAST(ACTOR AS VARCHAR) " +
              "END AS ACTOR " +
              "FROM V3_TR_PART_ACT_INDIVIDUALJL " +
              "WHERE ACTOR IN (6,7) AND (" +
              "SEXO IS NOT NULL OR EDAD IS NOT NULL OR OCUPACION IS NOT NULL OR " +
              "NSS IS NOT NULL OR CURP IS NOT NULL OR RFC_TRABAJADOR IS NOT NULL OR JORNADA IS NOT NULL" +
              ")";

        //System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                // En tu clase original solo regresabas 3 columnas
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ACTOR")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Actor_otro()", ex);
        }

        return Array;
    }
}


