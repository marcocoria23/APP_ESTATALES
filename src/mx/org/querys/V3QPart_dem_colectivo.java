package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QPart_dem_colectivo {

    private static final Logger LOG = Logger.getLogger(V3QPart_dem_colectivo.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Query: Demandados NO desglosados (cuando debería)
    public ArrayList<String[]> ExpeNDesglose() {

        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN INCOMPETENCIA = 2 THEN 'NO' ELSE NULL END AS INCOMPETENCIA, " +
            "       CASE WHEN ESTATUS_DEMANDA = 1 THEN 'ADMITIDA' ELSE NULL END AS ESTATUS_DEMANDA, " +
            "       CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS " +
            "FROM V3_TR_COLECTIVOJL s " +
            "WHERE s.INCOMPETENCIA = 2 " +
            "  AND s.ESTATUS_DEMANDA = 1 " +
            "  AND s.CANTIDAD_DEMANDADOS > 0 " +
            "  AND s.EXPEDIENTE_CLAVE NOT IN ( " +
            "      SELECT EXPEDIENTE_CLAVE FROM V3_TR_PART_DEM_COLECTIVOJL " +
            "  )";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("ESTATUS_DEMANDA"),
                    rs.getString("CANTIDAD_ACTORES"),
                    rs.getString("CANTIDAD_DEMANDADOS")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en ExpeNDesglose()", ex);
        }

        return Array;
    }

    // Query: Incompetencia = Sí pero existe desglose de demandados
    public ArrayList<String[]> IncompetenciaNE() {

        Array = new ArrayList<>();

        sql =
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_COLECTIVO, x.INCOMPETENCIA " +
            "FROM ( " +
            "   SELECT p.CLAVE_ORGANO, " +
            "          p.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          s.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_COLECTIVO, " +
            "          CASE " +
            "             WHEN s.INCOMPETENCIA = 1 THEN 'Sí' " +
            "             WHEN s.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO, PERIODO " +
            "       FROM V3_TR_PART_DEM_COLECTIVOJL " +
            "   ) p " +
            "   LEFT JOIN V3_TR_COLECTIVOJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") x " +
            "WHERE x.INCOMPETENCIA = 'Sí'";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE_COLECTIVO"),
                    rs.getString("INCOMPETENCIA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en IncompetenciaNE()", ex);
        }

        return Array;
    }

    // Query: Estatus demanda NO permitido pero existe desglose de demandados
    public ArrayList<String[]> Estatus_demandaNE() {

        Array = new ArrayList<>();

        sql =
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_COLECTIVO, x.ESTATUS_DEMANDA " +
            "FROM ( " +
            "   SELECT p.CLAVE_ORGANO, " +
            "          p.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          s.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_COLECTIVO, " +
            "          CASE " +
            "             WHEN s.ESTATUS_DEMANDA = 1 THEN 'Admitida' " +
            "             WHEN s.ESTATUS_DEMANDA = 2 THEN 'Desechada' " +
            "             WHEN s.ESTATUS_DEMANDA = 3 THEN 'Archivo' " +
            "             WHEN s.ESTATUS_DEMANDA = 4 THEN 'No se dio trámite al escrito de demanda' " +
            "             ELSE NULL " +
            "          END AS ESTATUS_DEMANDA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO, PERIODO " +
            "       FROM V3_TR_PART_DEM_COLECTIVOJL " +
            "   ) p " +
            "   LEFT JOIN V3_TR_COLECTIVOJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") x " +
            "WHERE x.ESTATUS_DEMANDA IN ('Desechada','Archivo','No se dio trámite al escrito de demanda')";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE_COLECTIVO"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_demandaNE()", ex);
        }

        return Array;
    }

    // Query: Cantidad demandados != desglose demandados
    public ArrayList<String[]> Dif_demandadosNE() {

        Array = new ArrayList<>();

        sql =
            "SELECT * FROM ( " +
            "   SELECT p.CLAVE_ORGANO, p.EXPEDIENTE_CLAVE, " +
            "          COALESCE(s.CANTIDAD_DEMANDADOS, 0) AS CANTIDAD_DEMANDADOS, " +
            "          COALESCE(p.DESGLOSE_DEMANDADO, 0) AS DESGLOSE_DEMANDADO, " +
            "          COALESCE(CAST(s.INCOMPETENCIA AS VARCHAR), 'NULLO') AS INCOMPETENCIA, " +
            "          COALESCE(CAST(s.ESTATUS_DEMANDA AS VARCHAR), 'NULLO') AS ESTATUS_DEMANDA, " +
            "          p.PERIODO " +
            "   FROM ( " +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, COUNT(ID_DEMANDADO) AS DESGLOSE_DEMANDADO " +
            "       FROM V3_TR_PART_DEM_COLECTIVOJL " +
            "       WHERE ID_DEMANDADO NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO " +
            "   ) p " +
            "   LEFT JOIN V3_TR_COLECTIVOJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") x " +
            "WHERE x.INCOMPETENCIA <> '1' " +
            "  AND x.ESTATUS_DEMANDA NOT IN ('2','3','4') " +
            "  AND x.CANTIDAD_DEMANDADOS <> x.DESGLOSE_DEMANDADO";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CANTIDAD_DEMANDADOS"),
                    rs.getString("DESGLOSE_DEMANDADO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Dif_demandadosNE()", ex);
        }

        return Array;
    }
}
