package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3Part_Dem_huelga {

    private static final Logger LOGGER = Logger.getLogger(V3Part_Dem_huelga.class.getName());

    String sql;
    ArrayList<String[]> Array;

    /// Demandado no debe ser 9 = No identificado
    public ArrayList DemandadoNI() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN DEMANDADO = 9 THEN 'No identificado' ELSE CAST(DEMANDADO AS VARCHAR) END AS DEMANDADO, " +
              "PERIODO " +
              "FROM V3_TR_PART_DEM_HUELGAJL " +
              "WHERE DEMANDADO = 9";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("DEMANDADO"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en DemandadoNI()", ex);
        }

        return Array;
    }

    /// Cuando TIPO = Persona_fisica no debe capturar desde Razón social hasta Longitud
    public ArrayList Persona_fisica() {
        Array = new ArrayList<>();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
              "CASE WHEN TIPO = 1 THEN 'Persona_fisica' ELSE CAST(TIPO AS VARCHAR) END AS TIPO, " +
              "PERIODO " +
              "FROM V3_TR_PART_DEM_HUELGAJL " +
              "WHERE DEMANDADO = 1 AND TIPO = 1 AND (" +
              "RAZON_SOCIAL_EMPR IS NOT NULL OR CALLE IS NOT NULL OR N_EXT IS NOT NULL OR N_INT IS NOT NULL OR " +
              "COLONIA IS NOT NULL OR CP IS NOT NULL OR ENTIDAD_NOMBRE_EMPR IS NOT NULL OR ENTIDAD_CLAVE_EMPR IS NOT NULL OR " +
              "MUNICIPIO_NOMBRE_EMPR IS NOT NULL OR MUNICIPIO_CLAVE_EMPR IS NOT NULL OR " +
              "LATITUD_EMPR IS NOT NULL OR LONGITUD_EMPR IS NOT NULL" +
              ")";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error en Persona_fisica()", ex);
        }

        return Array;
    }
}
