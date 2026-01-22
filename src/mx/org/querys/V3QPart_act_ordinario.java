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
            "FROM V3_TR_ORDINARIOJL S " +
            "WHERE S.INCOMPETENCIA = 2 " +
            "  AND S.ESTATUS_DEMANDA = 1 " +
            "  AND S.CANTIDAD_ACTORES > 0 " +
            "  AND S.EXPEDIENTE_CLAVE NOT IN ( " +
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
            "SELECT X.CLAVE_ORGANO, X.EXPEDIENTE_CLAVE_ORDINARIO, X.INCOMPETENCIA " +
            "FROM ( " +
            "   SELECT P.CLAVE_ORGANO, " +
            "          P.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_ORDINARIO, " +
            "          CASE " +
            "             WHEN S.INCOMPETENCIA = 1 THEN 'SÍ' " +
            "             WHEN S.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO " +
            "       FROM V3_TR_PART_ACT_ORDINARIOJL " +
            "   ) P " +
            "   LEFT JOIN V3_TR_ORDINARIOJL S " +
            "     ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            "    AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            ") X " +
            "WHERE X.INCOMPETENCIA = 'SÍ'";

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
            "SELECT X.CLAVE_ORGANO, X.EXPEDIENTE_CLAVE_ORDINARIO, X.ESTATUS_DEMANDA " +
            "FROM ( " +
            "   SELECT P.CLAVE_ORGANO, " +
            "          P.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          S.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_ORDINARIO, " +
            "          CASE " +
            "             WHEN S.ESTATUS_DEMANDA = 1 THEN 'ADMITIDA' " +
            "             WHEN S.ESTATUS_DEMANDA = 2 THEN 'DESECHADA' " +
            "             WHEN S.ESTATUS_DEMANDA = 3 THEN 'ARCHIVO' " +
            "             WHEN S.ESTATUS_DEMANDA = 4 THEN 'NO SE DIO TRÁMITE AL ESCRITO DE DEMANDA' " +
            "             ELSE NULL " +
            "          END AS ESTATUS_DEMANDA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO " +
            "       FROM V3_TR_PART_ACT_ORDINARIOJL " +
            "   ) P " +
            "   LEFT JOIN V3_TR_ORDINARIOJL S " +
            "     ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            "    AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            ") X " +
            "WHERE X.ESTATUS_DEMANDA IN ('DESECHADA','ARCHIVO','NO SE DIO TRÁMITE AL ESCRITO DE DEMANDA')";

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
            "   SELECT P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, " +
            "          COALESCE(S.CANTIDAD_ACTORES, 0) AS CANTIDAD_ACTORES, " +
            "          COALESCE(P.DESGLOSE_ACTOR, 0) AS DESGLOSE_ACTORES, " +
            "          COALESCE(CAST(S.INCOMPETENCIA AS VARCHAR), 'NULLO') AS INCOMPETENCIA, " +
            "          COALESCE(CAST(S.ESTATUS_DEMANDA AS VARCHAR), 'NULLO') AS ESTATUS_DEMANDA " +
            "   FROM ( " +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COUNT(ID_ACTOR) AS DESGLOSE_ACTOR " +
            "       FROM V3_TR_PART_ACT_ORDINARIOJL " +
            "       WHERE ID_ACTOR NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE " +
            "   ) P " +
            "   LEFT JOIN V3_TR_ORDINARIOJL S " +
            "     ON P.CLAVE_ORGANO = S.CLAVE_ORGANO " +
            "    AND P.EXPEDIENTE_CLAVE = S.EXPEDIENTE_CLAVE " +
            ") X " +
            "WHERE X.INCOMPETENCIA <> '1' " +
            "  AND X.ESTATUS_DEMANDA NOT IN ('2','3','4') " +
            "  AND X.CANTIDAD_ACTORES <> X.DESGLOSE_ACTORES";

        System.out.println(sql);
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
