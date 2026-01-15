package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QPart_act_colectivo {

    private static final Logger LOG = Logger.getLogger(V3QPart_act_colectivo.class.getName());

    private String sql;
    private ArrayList<String[]> Array;

    // =========================================================
    // 1) Expediente NO desglosado (actor/demandado) (sin filtros)
    // =========================================================
    public ArrayList<String[]> ExpeNDesglose(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN INCOMPETENCIA = 2 THEN 'NO' ELSE NULL END AS INCOMPETENCIA, " +
            "       CASE WHEN ESTATUS_DEMANDA = 1 THEN 'ADMITIDA' ELSE NULL END AS ESTATUS_DEMANDA, " +
            "       CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS " +
            "FROM (" +
            "   SELECT * " +
            "   FROM V3_TR_COLECTIVOJL " +
            "   WHERE INCOMPETENCIA = 2 AND ESTATUS_DEMANDA = 1 AND CANTIDAD_ACTORES > 0" +
            ") c " +
            "WHERE c.EXPEDIENTE_CLAVE NOT IN (" +
            "   SELECT p.EXPEDIENTE_CLAVE " +
            "   FROM V3_TR_PART_ACT_COLECTIVOJL p" +
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
            LOG.log(Level.SEVERE, "Error en ExpeNDesglose", ex);
        }

        return Array;
    }

    // =========================================================
    // 2) Incompetencia = 'Sí' pero existe desglose (sin filtros)
    // =========================================================
    public ArrayList<String[]> IncompetenciaNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_COLECTIVO, x.INCOMPETENCIA " +
            "FROM (" +
            "   SELECT PRIN.CLAVE_ORGANO, " +
            "          PRIN.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          SEC.EXPEDIENTE_CLAVE  AS EXPEDIENTE_CLAVE_COLECTIVO, " +
            "          CASE " +
            "             WHEN SEC.INCOMPETENCIA = 1 THEN 'Sí' " +
            "             WHEN SEC.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM (" +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO " +
            "       FROM V3_TR_PART_ACT_COLECTIVOJL" +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_COLECTIVOJL SEC " +
            "     ON PRIN.CLAVE_ORGANO = SEC.CLAVE_ORGANO " +
            "    AND PRIN.EXPEDIENTE_CLAVE = SEC.EXPEDIENTE_CLAVE " +
            ") x " +
            "WHERE x.INCOMPETENCIA = 'Sí'";

        try (
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
            LOG.log(Level.SEVERE, "Error en IncompetenciaNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 3) Estatus demanda (Desechada/Archivo/No trámite) pero hay desglose
    // =========================================================
    public ArrayList<String[]> Estatus_demandaNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_COLECTIVO, x.ESTATUS_DEMANDA " +
            "FROM (" +
            "   SELECT PRIN.CLAVE_ORGANO, " +
            "          PRIN.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          SEC.EXPEDIENTE_CLAVE  AS EXPEDIENTE_CLAVE_COLECTIVO, " +
            "          CASE " +
            "             WHEN SEC.ESTATUS_DEMANDA = 1 THEN 'Admitida' " +
            "             WHEN SEC.ESTATUS_DEMANDA = 2 THEN 'Desechada' " +
            "             WHEN SEC.ESTATUS_DEMANDA = 3 THEN 'Archivo' " +
            "             WHEN SEC.ESTATUS_DEMANDA = 4 THEN 'No se dio trámite al escrito de demanda' " +
            "             ELSE NULL " +
            "          END AS ESTATUS_DEMANDA " +
            "   FROM (" +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO " +
            "       FROM V3_TR_PART_ACT_COLECTIVOJL" +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_COLECTIVOJL SEC " +
            "     ON PRIN.CLAVE_ORGANO = SEC.CLAVE_ORGANO " +
            "    AND PRIN.EXPEDIENTE_CLAVE = SEC.EXPEDIENTE_CLAVE " +
            ") x " +
            "WHERE x.ESTATUS_DEMANDA IN (" +
            "   'Desechada','Archivo','No se dio trámite al escrito de demanda'" +
            ")";

        try (
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
            LOG.log(Level.SEVERE, "Error en Estatus_demandaNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 4) Cantidad actores != desglose actores (sin filtros)
    // =========================================================
    public ArrayList<String[]> Dif_ActoresNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT * FROM (" +
            "   SELECT PRIN.CLAVE_ORGANO, PRIN.EXPEDIENTE_CLAVE, " +
            "          CASE WHEN SEC.CANTIDAD_ACTORES IS NULL THEN 0 ELSE SEC.CANTIDAD_ACTORES END AS CANTIDAD_ACTORES, " +
            "          CASE WHEN PRIN.DESGLOSE_ACTOR IS NULL THEN 0 ELSE PRIN.DESGLOSE_ACTOR END AS DESGLOSE_ACTORES, " +
            "          CASE WHEN SEC.INCOMPETENCIA IS NULL THEN 'NULLO' ELSE CAST(SEC.INCOMPETENCIA AS VARCHAR) END AS INCOMPETENCIA, " +
            "          CASE WHEN SEC.ESTATUS_DEMANDA IS NULL THEN 'NULLO' ELSE CAST(SEC.ESTATUS_DEMANDA AS VARCHAR) END AS ESTATUS_DEMANDA, " +
            "   FROM (" +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COUNT(ID_ACTOR) AS DESGLOSE_ACTOR " +
            "       FROM V3_TR_PART_ACT_COLECTIVOJL " +
            "       WHERE ID_ACTOR NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE" +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_COLECTIVOJL SEC " +
            "     ON PRIN.CLAVE_ORGANO = SEC.CLAVE_ORGANO " +
            "    AND PRIN.EXPEDIENTE_CLAVE = SEC.EXPEDIENTE_CLAVE " +
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
            LOG.log(Level.SEVERE, "Error en Dif_ActoresNE", ex);
        }

        return Array;
    }
}
