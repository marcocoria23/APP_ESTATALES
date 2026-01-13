package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Part_Act_ordinario {

    private static final Logger LOGGER = Logger.getLogger(V3Part_Act_ordinario.class.getName());

    String sql;
    ArrayList<String[]> Array;

    /// Actor no debe ser No identificado
    public ArrayList ActorNI() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 99 THEN 'No identificado' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR, " +
              "PERIODO " +
              "FROM V3_TR_PART_ACT_ORDINARIOJL " +
              "WHERE ACTOR = 99";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ACTOR"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en ActorNI()", ex);
        }

        return Array;
    }

    /// CUANDO ACTOR = Trabajador no debe capturar desde Nombre del sindicato hasta No_identificado.
    public ArrayList Actor_Trabajador() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 1 THEN 'Trabajador' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR, " +
              "PERIODO " +
              "FROM V3_TR_PART_ACT_ORDINARIOJL " +
              "WHERE ACTOR = 1 AND (" +
              "NOMBRE_SINDICATO IS NOT NULL OR REG_ASOC_SINDICAL IS NOT NULL OR " +
              "TIPO_SINDICATO IS NOT NULL OR OTRO_ESP_SINDICATO IS NOT NULL OR ORG_OBRERA IS NOT NULL OR " +
              "NOMBRE_ORG_OBRERA IS NOT NULL OR OTRO_ESP_OBRERA IS NOT NULL OR " +
              "HOMBRES IS NOT NULL OR MUJERES IS NOT NULL OR NO_IDENTIFICADO IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ACTOR"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Actor_Trabajador()", ex);
        }

        return Array;
    }

    /// CUANDO ACTOR = Sindicato no debe capturar desde Sexo hasta Jornada.
    public ArrayList Actor_Sindicato() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 3 THEN 'Sindicato' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR, " +
              "PERIODO " +
              "FROM V3_TR_PART_ACT_ORDINARIOJL " +
              "WHERE ACTOR = 3 AND (" +
              "SEXO IS NOT NULL OR EDAD IS NOT NULL OR OCUPACION IS NOT NULL OR NSS IS NOT NULL OR " +
              "CURP IS NOT NULL OR RFC_TRABAJADOR IS NOT NULL OR JORNADA IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ACTOR"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Actor_Sindicato()", ex);
        }

        return Array;
    }

    /// CUANDO ACTOR = Coalicion_de_trabajadores no debe capturar desde Sexo hasta Jornada.
    public ArrayList Actor_Coalicion() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 4 THEN 'Coalicion_de_trabajadores' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR, " +
              "PERIODO " +
              "FROM V3_TR_PART_ACT_ORDINARIOJL " +
              "WHERE ACTOR = 4 AND (" +
              "SEXO IS NOT NULL OR EDAD IS NOT NULL OR OCUPACION IS NOT NULL OR NSS IS NOT NULL OR " +
              "CURP IS NOT NULL OR RFC_TRABAJADOR IS NOT NULL OR JORNADA IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ACTOR"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Actor_Coalicion()", ex);
        }

        return Array;
    }
}
