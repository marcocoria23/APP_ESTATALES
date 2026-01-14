package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QPart_dem_nat_eco {

    private static final Logger LOG = Logger.getLogger(V3QPart_dem_nat_eco.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Query: Demandados NO desglosados (cuando debería)
    public ArrayList<String[]> ExpeNDesglose(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN INCOMPETENCIA = 2 THEN 'NO' ELSE NULL END AS INCOMPETENCIA, " +
            "       CASE WHEN ESTATUS_DEMANDA = 1 THEN 'ADMITIDA' ELSE NULL END AS ESTATUS_DEMANDA, " +
            "       CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS " +
            "FROM ( " +
            "   SELECT * " +
            "   FROM V3_TR_COLECT_ECONOMJL " +
            "   WHERE INCOMPETENCIA = 2 " +
            "     AND ESTATUS_DEMANDA = 1 " +
            "     AND CANTIDAD_DEMANDADOS > 0 " +
            ") x " +
            "WHERE x.EXPEDIENTE_CLAVE NOT IN ( " +
            "   SELECT EXPEDIENTE_CLAVE FROM V3_TR_PART_DEM_COLECT_ECONOMJL " +
            ")";

        try (
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
    public ArrayList<String[]> IncompetenciaNE(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT y.CLAVE_ORGANO, y.EXPEDIENTE_CLAVE_COLECT_ECONOM, y.INCOMPETENCIA " +
            "FROM ( " +
            "   SELECT p.CLAVE_ORGANO, " +
            "          p.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          s.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_COLECT_ECONOM, " +
            "          CASE " +
            "             WHEN s.INCOMPETENCIA = 1 THEN 'Sí' " +
            "             WHEN s.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO, PERIODO " +
            "       FROM V3_TR_PART_DEM_COLECT_ECONOMJL " +
            "   ) p " +
            "   LEFT JOIN V3_TR_COLECT_ECONOMJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") y " +
            "WHERE y.INCOMPETENCIA = 'Sí'";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE_COLECT_ECONOM"),
                    rs.getString("INCOMPETENCIA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en IncompetenciaNE()", ex);
        }

        return Array;
    }

    // Query: Cantidad demandados != desglose demandados (solo cuando incompetencia <> 1)
    public ArrayList<String[]> Dif_demandadosNE(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT * FROM ( " +
            "   SELECT p.CLAVE_ORGANO, p.EXPEDIENTE_CLAVE, " +
            "          COALESCE(s.CANTIDAD_DEMANDADOS, 0) AS CANTIDAD_DEMANDADOS, " +
            "          COALESCE(p.DESGLOSE_DEMANDADO, 0) AS DESGLOSE_DEMANDADO, " +
            "          COALESCE(CAST(s.INCOMPETENCIA AS VARCHAR), 'NULLO') AS INCOMPETENCIA, " +
            "          p.PERIODO " +
            "   FROM ( " +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, COUNT(ID_DEMANDADO) AS DESGLOSE_DEMANDADO " +
            "       FROM V3_TR_PART_DEM_COLECT_ECONOMJL " +
            "       WHERE ID_DEMANDADO NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO " +
            "   ) p " +
            "   LEFT JOIN V3_TR_COLECT_ECONOMJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") x " +
            "WHERE x.INCOMPETENCIA <> '1' " +
            "  AND x.CANTIDAD_DEMANDADOS <> x.DESGLOSE_DEMANDADO";

        try (
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
