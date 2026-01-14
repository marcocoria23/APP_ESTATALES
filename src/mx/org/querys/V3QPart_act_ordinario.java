package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QPart_act_ordinario {

    private static final Logger LOG = Logger.getLogger(V3QPart_act_ordinario.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // =========================================================
    // 1) Expediente NO desglosado (actor/demandado) - ORDINARIO
    // =========================================================
    public ArrayList<String[]> ExpeNDesglose(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN INCOMPETENCIA = 2 THEN 'NO' ELSE NULL END AS INCOMPETENCIA, " +
            "       CASE WHEN ESTATUS_DEMANDA = 1 THEN 'ADMITIDA' ELSE NULL END AS ESTATUS_DEMANDA, " +
            "       CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS " +
            "FROM V3_TR_ORDINARIOJL s " +
            "WHERE s.INCOMPETENCIA = 2 " +
            "  AND s.ESTATUS_DEMANDA = 1 " +
            "  AND s.CANTIDAD_ACTORES > 0 " +
            "  AND s.EXPEDIENTE_CLAVE NOT IN ( " +
            "      SELECT EXPEDIENTE_CLAVE FROM V3_TR_PART_ACT_ORDINARIOJL " +
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
    // 2) Incompetencia = 'Sí' pero existe desglose - ORDINARIO
    // =========================================================
    public ArrayList<String[]> IncompetenciaNE(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_ORDINARIO, x.INCOMPETENCIA " +
            "FROM ( " +
            "   SELECT p.CLAVE_ORGANO, " +
            "          p.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          s.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_ORDINARIO, " +
            "          CASE " +
            "             WHEN s.INCOMPETENCIA = 1 THEN 'Sí' " +
            "             WHEN s.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO, PERIODO " +
            "       FROM V3_TR_PART_ACT_ORDINARIOJL " +
            "   ) p " +
            "   LEFT JOIN V3_TR_ORDINARIOJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") x " +
            "WHERE x.INCOMPETENCIA = 'Sí'";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE_ORDINARIO"),
                    rs.getString("INCOMPETENCIA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en IncompetenciaNE()", ex);
        }

        return Array;
    }

    // =========================================================
    // 3) Estatus demanda NO permitido pero existe desglose - ORDINARIO
    // =========================================================
    public ArrayList<String[]> Estatus_demandaNE(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_ORDINARIO, x.ESTATUS_DEMANDA " +
            "FROM ( " +
            "   SELECT p.CLAVE_ORGANO, " +
            "          p.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          s.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_ORDINARIO, " +
            "          CASE " +
            "             WHEN s.ESTATUS_DEMANDA = 1 THEN 'Admitida' " +
            "             WHEN s.ESTATUS_DEMANDA = 2 THEN 'Desechada' " +
            "             WHEN s.ESTATUS_DEMANDA = 3 THEN 'Archivo' " +
            "             WHEN s.ESTATUS_DEMANDA = 4 THEN 'No se dio trámite al escrito de demanda' " +
            "             ELSE NULL " +
            "          END AS ESTATUS_DEMANDA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO, PERIODO " +
            "       FROM V3_TR_PART_ACT_ORDINARIOJL " +
            "   ) p " +
            "   LEFT JOIN V3_TR_ORDINARIOJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") x " +
            "WHERE x.ESTATUS_DEMANDA IN ('Desechada','Archivo','No se dio trámite al escrito de demanda')";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE_ORDINARIO"),
                    rs.getString("ESTATUS_DEMANDA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_demandaNE()", ex);
        }

        return Array;
    }

    // =========================================================
    // 4) Cantidad actores != desglose actores - ORDINARIO
    // =========================================================
    public ArrayList<String[]> Dif_ActoresNE(Connection con) {

        Array = new ArrayList<>();

        sql =
            "SELECT * FROM ( " +
            "   SELECT p.CLAVE_ORGANO, p.EXPEDIENTE_CLAVE, " +
            "          COALESCE(s.CANTIDAD_ACTORES, 0) AS CANTIDAD_ACTORES, " +
            "          COALESCE(p.DESGLOSE_ACTOR, 0) AS DESGLOSE_ACTORES, " +
            "          COALESCE(CAST(s.INCOMPETENCIA AS VARCHAR), 'NULLO') AS INCOMPETENCIA, " +
            "          COALESCE(CAST(s.ESTATUS_DEMANDA AS VARCHAR), 'NULLO') AS ESTATUS_DEMANDA, " +
            "          p.PERIODO " +
            "   FROM ( " +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, COUNT(ID_ACTOR) AS DESGLOSE_ACTOR " +
            "       FROM V3_TR_PART_ACT_ORDINARIOJL " +
            "       WHERE ID_ACTOR NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO " +
            "   ) p " +
            "   LEFT JOIN V3_TR_ORDINARIOJL s " +
            "     ON p.CLAVE_ORGANO = s.CLAVE_ORGANO " +
            "    AND p.EXPEDIENTE_CLAVE = s.EXPEDIENTE_CLAVE " +
            "    AND p.PERIODO = s.PERIODO " +
            ") x " +
            "WHERE x.INCOMPETENCIA <> '1' " +
            "  AND x.ESTATUS_DEMANDA NOT IN ('2','3','4') " +
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
