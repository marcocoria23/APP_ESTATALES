package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Part_Dem_colectivo {

    private static final Logger LOGGER = Logger.getLogger(V3Part_Dem_colectivo.class.getName());

    String sql;
    ArrayList<String[]> Array;

    /// Demandado no debe ser 9 = No identificado
    public ArrayList DemandadoNI(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN DEMANDADO = 9 THEN 'No identificado' ELSE CAST(DEMANDADO AS VARCHAR) END AS DEMANDADO " +
              "FROM V3_TR_PART_DEM_COLECTIVOJL " +
              "WHERE DEMANDADO = 9";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("DEMANDADO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en DemandadoNI()", ex);
        }

        return Array;
    }

    // Cuando Demandado = Otro no debe capturar desde Nombre del sindicato hasta Longitud
    public ArrayList Demandado_Otro(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN DEMANDADO = 5 THEN 'Otro' ELSE CAST(DEMANDADO AS VARCHAR) END AS DEMANDADO " +
              "FROM V3_TR_PART_DEM_COLECTIVOJL " +
              "WHERE DEMANDADO = 5 AND (" +
              "NOMBRE_SINDICATO_DEM IS NOT NULL OR REG_ASOC_SINDICAL_DEM IS NOT NULL OR TIPO_SINDICATO_DEM IS NOT NULL OR " +
              "OTRO_ESP_SINDICATO_DEM IS NOT NULL OR ORG_OBRERA_DEM IS NOT NULL OR NOMBRE_ORG_OBRERA_DEM IS NOT NULL OR " +
              "OTRO_ESP_OBRERA_DEM IS NOT NULL OR HOMBRES_DEM IS NOT NULL OR MUJERES_DEM IS NOT NULL OR NO_IDENTIFICADO_DEM IS NOT NULL OR " +
              "TIPO_DEM_PAT IS NOT NULL OR RFC_PATRON_DEM IS NOT NULL OR RAZON_SOCIAL_EMPR_DEM IS NOT NULL OR " +
              "CALLE IS NOT NULL OR N_EXT IS NOT NULL OR N_INT IS NOT NULL OR COLONIA IS NOT NULL OR CP IS NOT NULL OR " +
              "ENTIDAD_NOMBRE_EMPR IS NOT NULL OR ENTIDAD_CLAVE_EMPR IS NOT NULL OR MUNICIPIO_NOMBRE_EMPR IS NOT NULL OR " +
              "MUNICIPIO_CLAVE_EMPR IS NOT NULL OR LATITUD_EMPR IS NOT NULL OR LONGITUD_EMPR IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("DEMANDADO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Demandado_Otro()", ex);
        }

        return Array;
    }

    // Cuando Demandado = Sindicato no debe capturar desde tipo hasta longitud
    public ArrayList Demandado_Sindicato(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN DEMANDADO = 2 THEN 'Sindicato' ELSE CAST(DEMANDADO AS VARCHAR) END AS DEMANDADO " +
              "FROM V3_TR_PART_DEM_COLECTIVOJL " +
              "WHERE DEMANDADO = 2 AND (" +
              "TIPO_DEM_PAT IS NOT NULL OR RFC_PATRON_DEM IS NOT NULL OR RAZON_SOCIAL_EMPR_DEM IS NOT NULL OR " +
              "CALLE IS NOT NULL OR N_EXT IS NOT NULL OR N_INT IS NOT NULL OR COLONIA IS NOT NULL OR CP IS NOT NULL OR " +
              "ENTIDAD_NOMBRE_EMPR IS NOT NULL OR ENTIDAD_CLAVE_EMPR IS NOT NULL OR MUNICIPIO_NOMBRE_EMPR IS NOT NULL OR " +
              "MUNICIPIO_CLAVE_EMPR IS NOT NULL OR LATITUD_EMPR IS NOT NULL OR LONGITUD_EMPR IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("DEMANDADO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Demandado_Sindicato()", ex);
        }

        return Array;
    }

    // Cuando Demandado = Coalicion_de_trabajadores solo debe capturar Cantidad/Hombres/Mujeres/No_identificado (si trae otras, está mal)
    public ArrayList Demandado_Coalicion(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN DEMANDADO = 3 THEN 'Coalicion_de_trabajadores' ELSE CAST(DEMANDADO AS VARCHAR) END AS DEMANDADO " +
              "FROM V3_TR_PART_DEM_COLECTIVOJL " +
              "WHERE DEMANDADO = 3 AND (" +
              "NOMBRE_SINDICATO_DEM IS NOT NULL OR REG_ASOC_SINDICAL_DEM IS NOT NULL OR TIPO_SINDICATO_DEM IS NOT NULL OR " +
              "OTRO_ESP_SINDICATO_DEM IS NOT NULL OR ORG_OBRERA_DEM IS NOT NULL OR NOMBRE_ORG_OBRERA_DEM IS NOT NULL OR " +
              "OTRO_ESP_OBRERA_DEM IS NOT NULL OR TIPO_DEM_PAT IS NOT NULL OR RFC_PATRON_DEM IS NOT NULL OR RAZON_SOCIAL_EMPR_DEM IS NOT NULL OR " +
              "CALLE IS NOT NULL OR N_EXT IS NOT NULL OR N_INT IS NOT NULL OR COLONIA IS NOT NULL OR CP IS NOT NULL OR " +
              "ENTIDAD_NOMBRE_EMPR IS NOT NULL OR ENTIDAD_CLAVE_EMPR IS NOT NULL OR MUNICIPIO_NOMBRE_EMPR IS NOT NULL OR " +
              "MUNICIPIO_CLAVE_EMPR IS NOT NULL OR LATITUD_EMPR IS NOT NULL OR LONGITUD_EMPR IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("DEMANDADO") 
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Demandado_Coalicion()", ex);
        }

        return Array;
    }

    // Cuando Demandado = Patron no debe capturar desde Nombre del sindicato hasta No_identificado
    public ArrayList Demandado_Patron(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN DEMANDADO = 1 THEN 'Patron' ELSE CAST(DEMANDADO AS VARCHAR) END AS DEMANDADO  " + 
              "FROM V3_TR_PART_DEM_COLECTIVOJL " +
              "WHERE DEMANDADO = 1 AND (" +
              "NOMBRE_SINDICATO_DEM IS NOT NULL OR REG_ASOC_SINDICAL_DEM IS NOT NULL OR TIPO_SINDICATO_DEM IS NOT NULL OR " +
              "OTRO_ESP_SINDICATO_DEM IS NOT NULL OR ORG_OBRERA_DEM IS NOT NULL OR NOMBRE_ORG_OBRERA_DEM IS NOT NULL OR " +
              "OTRO_ESP_OBRERA_DEM IS NOT NULL OR HOMBRES_DEM IS NOT NULL OR MUJERES_DEM IS NOT NULL OR NO_IDENTIFICADO_DEM IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("DEMANDADO") 
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Demandado_Patron()", ex);
        }

        return Array;
    }

    // Cuando Demandado = Patron y tipo = persona_Fisica no debe capturar desde Razon social hasta Longitud
    // (NOTA: en tu SQL original estás usando TIPO_SINDICATO_DEM; lo dejo igual)
    public ArrayList persona_fisica(Connection con) {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO_SINDICATO_DEM = 1 THEN 'persona_Fisica' ELSE CAST(TIPO_SINDICATO_DEM AS VARCHAR) END AS TIPO_SINDICATO_DEM  " + 
              "FROM V3_TR_PART_DEM_COLECTIVOJL " +
              "WHERE DEMANDADO = 1 AND TIPO_SINDICATO_DEM = 1 AND (" +
              "RAZON_SOCIAL_EMPR_DEM IS NOT NULL OR CALLE IS NOT NULL OR N_EXT IS NOT NULL OR N_INT IS NOT NULL OR COLONIA IS NOT NULL OR " +
              "CP IS NOT NULL OR ENTIDAD_NOMBRE_EMPR IS NOT NULL OR ENTIDAD_CLAVE_EMPR IS NOT NULL OR MUNICIPIO_NOMBRE_EMPR IS NOT NULL OR " +
              "MUNICIPIO_CLAVE_EMPR IS NOT NULL OR LATITUD_EMPR IS NOT NULL OR LONGITUD_EMPR IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_SINDICATO_DEM") 
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en persona_fisica()", ex);
        }

        return Array;
    }
}
