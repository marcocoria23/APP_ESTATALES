package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QPart_act_nat_eco {

    private static final Logger LOG = Logger.getLogger(V3QPart_act_nat_eco.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // =========================================================
    // 1) Expediente NO desglosado (actor/demandado) - NAT ECO
    // =========================================================
    public ArrayList<String[]> ExpeNDesglose(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN INCOMPETENCIA = 2 THEN 'NO' ELSE NULL END AS INCOMPETENCIA, " +
            "       CASE WHEN ESTATUS_DEMANDA = 1 THEN 'ADMITIDA' ELSE NULL END AS ESTATUS_DEMANDA, " +
            "       CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS " +
            "FROM V3_TR_COLECT_ECONOMJL s " +
            "WHERE s.INCOMPETENCIA = 2 " +
            "  AND s.ESTATUS_DEMANDA = 1 " +
            "  AND s.CANTIDAD_ACTORES > 0 " +
            "  AND s.EXPEDIENTE_CLAVE NOT IN ( " +
            "      SELECT EXPEDIENTE_CLAVE FROM V3_TR_PART_ACT_COLECT_ECONOMJL " +
            "  )";

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

    // =========================================================
    // 2) Incompetencia = 'Sí' pero existe desglose - NAT ECO
    // =========================================================
    public ArrayList<String[]> IncompetenciaNE(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_COLECT_ECONOM, x.INCOMPETENCIA " +
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
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO " +
            "       FROM V3_TR_PART_ACT_COLECT_ECONOMJL " +
            "   ) p " +
            "   LEFT JOIN V3_TR_COLECT_ECONOMJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            ") x " +
            "WHERE x.INCOMPETENCIA = 'Sí'";

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

    // =========================================================
    // 3) Cantidad de actores != desglose de actores - NAT ECO
    // =========================================================
    public ArrayList<String[]> Dif_ActoresNE(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT * FROM ( " +
            "   SELECT p.CLAVE_ORGANO, p.EXPEDIENTE_CLAVE, " +
            "          COALESCE(s.CANTIDAD_ACTORES, 0) AS CANTIDAD_ACTORES, " +
            "          COALESCE(p.DESGLOSE_ACTOR, 0) AS DESGLOSE_ACTORES, " +
            "          COALESCE(CAST(s.INCOMPETENCIA AS VARCHAR), 'NULLO') AS INCOMPETENCIA, " +
            "   FROM ( " +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COUNT(ID_ACTOR) AS DESGLOSE_ACTOR " +
            "       FROM V3_TR_PART_ACT_COLECT_ECONOMJL " +
            "       WHERE ID_ACTOR NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE " +
            "   ) p " +
            "   LEFT JOIN V3_TR_COLECT_ECONOMJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            ") x " +
            "WHERE x.INCOMPETENCIA <> '1' " +
            "  AND x.CANTIDAD_ACTORES <> x.DESGLOSE_ACTORES";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("CANTIDAD_ACTORES"),
                    rs.getString("DESGLOSE_ACTORES")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Dif_ActoresNE()", ex);
        }

        return Array;
    }
}
