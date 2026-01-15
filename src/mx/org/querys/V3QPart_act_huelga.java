package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QPart_act_huelga {

    private static final Logger LOG = Logger.getLogger(V3QPart_act_huelga.class.getName());

    private String sql;
    private ArrayList<String[]> Array;

    // =========================================================
    // 1) Expediente NO desglosado (actor/demandado) (sin filtros)
    //    - INCOMPETENCIA = 2
    //    - CANTIDAD_ACTORES > 0
    //    - NO existe en V3_TR_PART_ACT_HUELGAJL
    //    - FASE_SOLI_EXPEDIENTE <> 5
    // =========================================================
    public ArrayList<String[]> ExpeNDesglose(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN INCOMPETENCIA = 2 THEN 'NO' ELSE NULL END AS INCOMPETENCIA, " +
            "       CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS " +
            "FROM V3_TR_HUELGAJL h " +
            "WHERE h.INCOMPETENCIA = 2 " +
            "  AND h.CANTIDAD_ACTORES > 0 " +
            "  AND h.FASE_SOLI_EXPEDIENTE <> 5 " +
            "  AND h.EXPEDIENTE_CLAVE NOT IN (" +
            "      SELECT p.EXPEDIENTE_CLAVE " +
            "      FROM V3_TR_PART_ACT_HUELGAJL p" +
            "  )";

        try (
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
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
            "SELECT x.CLAVE_ORGANO, x.EXPEDIENTE_CLAVE_HUELGA, x.INCOMPETENCIA " +
            "FROM (" +
            "   SELECT PRIN.CLAVE_ORGANO, " +
            "          PRIN.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          SEC.EXPEDIENTE_CLAVE  AS EXPEDIENTE_CLAVE_HUELGA, " +
            "          CASE " +
            "             WHEN SEC.INCOMPETENCIA = 1 THEN 'Sí' " +
            "             WHEN SEC.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM (" +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO " +
            "       FROM V3_TR_PART_ACT_HUELGAJL" +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_HUELGAJL SEC " +
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
                    rs.getString("EXPEDIENTE_CLAVE_HUELGA"),
                    rs.getString("INCOMPETENCIA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en IncompetenciaNE", ex);
        }

        return Array;
    }

    // =========================================================
    // 3) Cantidad actores != desglose actores (sin filtros)
    //    - INCOMPETENCIA <> 1
    //    - FASE_SOLI_EXPEDIENTE <> 5
    // =========================================================
    public ArrayList<String[]> Dif_ActoresNE(Connection con) {
        Array = new ArrayList<>();

        sql =
            "SELECT * FROM (" +
            "   SELECT PRIN.CLAVE_ORGANO, PRIN.EXPEDIENTE_CLAVE, " +
            "          COALESCE(SEC.CANTIDAD_ACTORES, 0) AS CANTIDAD_ACTORES, " +
            "          COALESCE(PRIN.DESGLOSE_ACTOR, 0) AS DESGLOSE_ACTORES, " +
            "          COALESCE(CAST(SEC.INCOMPETENCIA AS VARCHAR), '0') AS INCOMPETENCIA, " +
            "          COALESCE(CAST(SEC.FASE_SOLI_EXPEDIENTE AS VARCHAR), '0') AS FASE_SOLI_EXPEDIENTE " +
            "   FROM (" +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COUNT(ID_ACTOR) AS DESGLOSE_ACTOR " +
            "       FROM V3_TR_PART_ACT_HUELGAJL " +
            "       WHERE ID_ACTOR NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE" +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_HUELGAJL SEC " +
            "     ON PRIN.CLAVE_ORGANO = SEC.CLAVE_ORGANO " +
            "    AND PRIN.EXPEDIENTE_CLAVE = SEC.EXPEDIENTE_CLAVE " +
            ") x " +
            "WHERE x.INCOMPETENCIA <> '1' " +
            "  AND x.FASE_SOLI_EXPEDIENTE <> '5' " +
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
