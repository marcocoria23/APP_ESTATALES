package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Part_Act_colectivo {

    private static final Logger LOGGER = Logger.getLogger(V3Part_Act_colectivo.class.getName());

    String sql;
    ArrayList<String[]> Array;

    /// Actor no debe ser No identificado
    public ArrayList ActorNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 99 THEN 'No identificado' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR " +
              "FROM V3_TR_PART_ACT_COLECTIVOJL " +
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

    // CUANDO ACTOR = Sindicato no debe capturar desde Tipo hasta Longitud
    public ArrayList Actor_Sindicato(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 3 THEN 'Sindicato' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR " +
              "FROM V3_TR_PART_ACT_COLECTIVOJL " +
              "WHERE ACTOR = 3 AND (" +
              "TIPO IS NOT NULL OR RFC_PATRON IS NOT NULL OR RAZON_SOCIAL_EMPR IS NOT NULL OR CALLE IS NOT NULL OR " +
              "N_EXT IS NOT NULL OR N_INT IS NOT NULL OR COLONIA IS NOT NULL OR CP IS NOT NULL OR ENTIDAD_NOMBRE_EMPR IS NOT NULL OR " +
              "ENTIDAD_CLAVE_EMPR IS NOT NULL OR MUNICIPIO_NOMBRE_EMPR IS NOT NULL OR MUNICIPIO_CLAVE_EMPR IS NOT NULL OR " +
              "LATITUD_EMPR IS NOT NULL OR LONGITUD_EMPR IS NOT NULL" +
              ")";

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
            LOGGER.log(Level.SEVERE, "Error en Actor_Sindicato()", ex);
        }

        return Array;
    }

    // CUANDO ACTOR = Patron no debe capturar desde Nombre del sindicato hasta No_identificado.
    public ArrayList Actor_Patron(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN ACTOR = 2 THEN 'Patron' ELSE CAST(ACTOR AS VARCHAR) END AS ACTOR " +
              "FROM V3_TR_PART_ACT_COLECTIVOJL " +
              "WHERE ACTOR = 2 AND (" +
              "NOMBRE_SINDICATO IS NOT NULL OR REG_ASOC_SINDICAL IS NOT NULL OR TIPO_SINDICATO IS NOT NULL OR " +
              "OTRO_ESP_SINDICATO IS NOT NULL OR ORG_OBRERA IS NOT NULL OR NOMBRE_ORG_OBRERA IS NOT NULL OR OTRO_ESP_OBRERA IS NOT NULL OR " +
              "HOMBRES IS NOT NULL OR MUJERES IS NOT NULL OR NO_IDENTIFICADO IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                // En tu clase original regresabas SOLO 3 columnas
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ACTOR")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Actor_Patron()", ex);
        }

        return Array;
    }

    // CUANDO TIPO = Persona_fisica no debe capturar desde Razon social hasta Longitud.
    public ArrayList Persona_Fisica(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO = 1 THEN 'Persona_fisica' ELSE CAST(TIPO AS VARCHAR) END AS TIPO, " +
              " ACTOR " +
              "FROM V3_TR_PART_ACT_COLECTIVOJL " +
              "WHERE ACTOR = 2 AND TIPO = 1 AND (" +
              "RAZON_SOCIAL_EMPR IS NOT NULL OR CALLE IS NOT NULL OR N_EXT IS NOT NULL OR N_INT IS NOT NULL OR " +
              "COLONIA IS NOT NULL OR CP IS NOT NULL OR ENTIDAD_NOMBRE_EMPR IS NOT NULL OR ENTIDAD_CLAVE_EMPR IS NOT NULL OR " +
              "MUNICIPIO_NOMBRE_EMPR IS NOT NULL OR MUNICIPIO_CLAVE_EMPR IS NOT NULL OR LATITUD_EMPR IS NOT NULL OR LONGITUD_EMPR IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                // En tu clase original regresabas (TIPO, ACTOR) además de claves
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO"),
                    rs.getString("ACTOR")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Persona_Fisica()", ex);
        }

        return Array;
    }
}
