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
            "FROM V3_TR_COLECT_ECONOMJL S " +
            "WHERE S.INCOMPETENCIA = 2 " +
            "  AND S.ESTATUS_DEMANDA = 1 " +
            "  AND S.CANTIDAD_ACTORES > 0 " +
            "  AND S.EXPEDIENTE_CLAVE NOT IN ( " +
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
            "SELECT X.CLAVE_ORGANO, X.EXPEDIENTE_CLAVE_COLECT_ECONOM, X.INCOMPETENCIA " +
            "FROM ( " +
            "   SELECT P.CLAVE_ORGANO, " +
            "          P.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_COLECT_ECONOM, " +
            "          CASE " +
            "             WHEN S.INCOMPETENCIA = 1 THEN 'Sí' " +
            "             WHEN S.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO " +
            "       FROM V3_TR_PART_ACT_COLECT_ECONOMJL " +
            "   ) P " +
            "   LEFT JOIN V3_TR_COLECT_ECONOMJL S " +
            "     ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            "    AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            ") X " +
            "WHERE X.INCOMPETENCIA = 'Sí'";

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
            "   SELECT P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "          COALESCE(S.CANTIDAD_ACTORES, 0) AS CANTIDAD_ACTORES, " +
            "          COALESCE(P.DESGLOSE_ACTOR, 0) AS DESGLOSE_ACTORES, " +
            "          COALESCE(CAST(S.INCOMPETENCIA AS VARCHAR), 'NULLO') AS INCOMPETENCIA " +
            "   FROM ( " +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COUNT(ID_ACTOR) AS DESGLOSE_ACTOR " +
            "       FROM V3_TR_PART_ACT_COLECT_ECONOMJL " +
            "       WHERE ID_ACTOR NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE " +
            "   ) P " +
            "   LEFT JOIN V3_TR_COLECT_ECONOMJL S " +
            "     ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            "    AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            ") X " +
            "WHERE X.INCOMPETENCIA <> '1' " +
            "  AND X.CANTIDAD_ACTORES <> X.DESGLOSE_ACTORES";

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
