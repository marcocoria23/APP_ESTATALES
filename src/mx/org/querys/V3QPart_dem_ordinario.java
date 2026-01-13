package mx.org.querys;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class V3QPart_dem_ordinario {

    private static final Logger LOG = Logger.getLogger(V3QPart_dem_ordinario.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Query: Demandados NO desglosados (cuando debería)
    public ArrayList ExpeNDesglose() {
        Array = new ArrayList<>();

        sql =
            "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, " +
            "       CASE WHEN INCOMPETENCIA = 2 THEN 'NO' ELSE NULL END AS INCOMPETENCIA, " +
            "       CASE WHEN ESTATUS_DEMANDA = 1 THEN 'ADMITIDA' ELSE NULL END AS ESTATUS_DEMANDA, " +
            "       CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS " +
            "FROM ( " +
            "   SELECT * " +
            "   FROM V3_TR_ORDINARIOJL " +
            "   WHERE INCOMPETENCIA = 2 " +
            "     AND ESTATUS_DEMANDA = 1 " +
            "     AND CANTIDAD_DEMANDADOS > 0 " +
            ") X " +
            "WHERE X.EXPEDIENTE_CLAVE NOT IN ( " +
            "   SELECT EXPEDIENTE_CLAVE FROM V3_TR_PART_DEM_ORDINARIOJL " +
            ")";

        System.out.println(sql);

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
    public ArrayList IncompetenciaNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT * FROM ( " +
            "   SELECT PRIN.CLAVE_ORGANO, " +
            "          PRIN.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          SEC.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_ORDINARIO, " +
            "          CASE " +
            "             WHEN SEC.INCOMPETENCIA = 1 THEN 'Sí' " +
            "             WHEN SEC.INCOMPETENCIA = 2 THEN 'NO' " +
            "             ELSE NULL " +
            "          END AS INCOMPETENCIA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO, PERIODO " +
            "       FROM V3_TR_PART_DEM_ORDINARIOJL " +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_ORDINARIOJL SEC " +
            "     ON PRIN.CLAVE_ORGANO = SEC.CLAVE_ORGANO " +
            "    AND PRIN.EXPEDIENTE_CLAVE = SEC.EXPEDIENTE_CLAVE " +
            "    AND PRIN.PERIODO = SEC.PERIODO " +
            ") Z " +
            "WHERE INCOMPETENCIA = 'Sí'";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    // Query: Estatus demanda inválido (Desechada/Archivo/No trámite) pero hay desglose
    public ArrayList Estatus_demandaNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT * FROM ( " +
            "   SELECT PRIN.CLAVE_ORGANO, " +
            "          PRIN.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_PART, " +
            "          SEC.EXPEDIENTE_CLAVE AS EXPEDIENTE_CLAVE_ORDINARIO, " +
            "          CASE " +
            "             WHEN SEC.ESTATUS_DEMANDA = 1 THEN 'Admitida' " +
            "             WHEN SEC.ESTATUS_DEMANDA = 2 THEN 'Desechada' " +
            "             WHEN SEC.ESTATUS_DEMANDA = 3 THEN 'Archivo' " +
            "             WHEN SEC.ESTATUS_DEMANDA = 4 THEN 'No se dio trámite al escrito de demanda' " +
            "             ELSE NULL " +
            "          END AS ESTATUS_DEMANDA " +
            "   FROM ( " +
            "       SELECT DISTINCT EXPEDIENTE_CLAVE, CLAVE_ORGANO, PERIODO " +
            "       FROM V3_TR_PART_DEM_ORDINARIOJL " +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_ORDINARIOJL SEC " +
            "     ON PRIN.CLAVE_ORGANO = SEC.CLAVE_ORGANO " +
            "    AND PRIN.EXPEDIENTE_CLAVE = SEC.EXPEDIENTE_CLAVE " +
            "    AND PRIN.PERIODO = SEC.PERIODO " +
            ") Z " +
            "WHERE ESTATUS_DEMANDA IN ('Desechada','Archivo','No se dio trámite al escrito de demanda')";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
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

    // Query: Cantidad demandados != Desglose demandados (sin filtros)
    public ArrayList Dif_demandadosNE() {
        Array = new ArrayList<>();

        sql =
            "SELECT * FROM ( " +
            "   SELECT PRIN.CLAVE_ORGANO, PRIN.EXPEDIENTE_CLAVE, " +
            "          CASE WHEN SEC.CANTIDAD_DEMANDADOS IS NULL THEN 0 ELSE SEC.CANTIDAD_DEMANDADOS END AS CANTIDAD_DEMANDADOS, " +
            "          CASE WHEN PRIN.DESGLOSE_DEMANDADO IS NULL THEN 0 ELSE PRIN.DESGLOSE_DEMANDADO END AS DESGLOSE_DEMANDADO, " +
            "          CASE WHEN SEC.INCOMPETENCIA IS NULL THEN 'NULLO' ELSE CAST(SEC.INCOMPETENCIA AS VARCHAR) END AS INCOMPETENCIA, " +
            "          CASE WHEN SEC.ESTATUS_DEMANDA IS NULL THEN 'NULLO' ELSE CAST(SEC.ESTATUS_DEMANDA AS VARCHAR) END AS ESTATUS_DEMANDA, " +
            "          PRIN.PERIODO " +
            "   FROM ( " +
            "       SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO, COUNT(ID_DEMANDADO) AS DESGLOSE_DEMANDADO " +
            "       FROM V3_TR_PART_DEM_ORDINARIOJL " +
            "       WHERE ID_DEMANDADO NOT LIKE '%-%' " +
            "       GROUP BY CLAVE_ORGANO, EXPEDIENTE_CLAVE, PERIODO " +
            "   ) PRIN " +
            "   LEFT JOIN V3_TR_ORDINARIOJL SEC " +
            "     ON PRIN.CLAVE_ORGANO = SEC.CLAVE_ORGANO " +
            "    AND PRIN.EXPEDIENTE_CLAVE = SEC.EXPEDIENTE_CLAVE " +
            "    AND PRIN.PERIODO = SEC.PERIODO " +
            ") Z " +
            "WHERE INCOMPETENCIA <> '1' " +
            "  AND ESTATUS_DEMANDA NOT IN ('2','3','4') " +
            "  AND CANTIDAD_DEMANDADOS <> DESGLOSE_DEMANDADO";

        System.out.println(sql);

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
