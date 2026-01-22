package QuerysNuevosVal;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class QNuevos {

    // Helper único: ejecuta SQL y regresa CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS
    private ArrayList<String[]> ejecutar3Cols(String sql) {
        ArrayList<String[]> array = new ArrayList<>();
        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("COMENTARIOS")
                });
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return array;
    }

    // ---------------- ORDINARIO ----------------
    public ArrayList<String[]> OrdinarioEstatusFE() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_ORDINARIOJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 9\n"
                   + "AND (FECHA_DICTO_RESOLUCIONFE IS NULL OR FECHA_DICTO_RESOLUCIONFE = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> OrdinarioEstatusAP() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_ORDINARIOJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 1\n"
                   + "AND (FECHA_DICTO_RESOLUCIONAP IS NULL OR FECHA_DICTO_RESOLUCIONAP = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> OrdinarioEstatusAJ() {
        // ✅ corregido: antes tenías FECHA_DICTO_RESOLUCIONAP por error
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_ORDINARIOJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 2\n"
                   + "AND (FECHA_RESOLUCIONAJ IS NULL OR FECHA_RESOLUCIONAJ = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    // ---------------- INDIVIDUAL ----------------
    public ArrayList<String[]> IndividualEstatusAD() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 3\n"
                   + "AND (FECHA_DICTO_RESOLUCION_AD IS NULL OR FECHA_DICTO_RESOLUCION_AD = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualEstatusTA() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 4\n"
                   + "AND (FECHA_RESOLUCION_TA IS NULL OR FECHA_RESOLUCION_TA = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualEstatusAP() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 1\n"
                   + "AND (FECHA_DICTO_RESOLUCION_AP IS NULL OR FECHA_DICTO_RESOLUCION_AP = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualEstatusAJ() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 2\n"
                   + "AND (FECHA_DICTO_RESOLUCION_AJ IS NULL OR FECHA_DICTO_RESOLUCION_AJ = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    // ---------------- COLECTIVO ----------------
    public ArrayList<String[]> ColectivoEstatusAD() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECTIVOJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 3\n"
                   + "AND (FECHA_DICTO_RESOLUCION_AD IS NULL OR FECHA_DICTO_RESOLUCION_AD = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> ColectivoEstatusAJ() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECTIVOJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 2\n"
                   + "AND (FECHA_RESOLUCION_AJ IS NULL OR FECHA_RESOLUCION_AJ = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    // ---------------- HUELGA ----------------
    public ArrayList<String[]> HuelgaEstatusEH() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_HUELGAJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 5\n"
                   + "AND (FECHA_RESOLU_EMPLAZ IS NULL OR FECHA_RESOLU_EMPLAZ = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> HuelgaEstatusPH() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_HUELGAJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 6\n"
                   + "AND (FECHA_RESOLU_EMPLAZ IS NULL OR FECHA_RESOLU_EMPLAZ = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> HuelgaEstatusH() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_HUELGAJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 7\n"
                   + "AND (FECHA_RESOLU_HUELGA IS NULL OR FECHA_RESOLU_HUELGA = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    // ---------------- COLECT ECONOM / PARAPROCESAL / EJECUCION ----------------
    public ArrayList<String[]> Colect_EconomEstatusCE() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECT_ECONOMJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND FASE_SOLI_EXPEDIENTE = 8\n"
                   + "AND (FECHA_RESOLUCION IS NULL OR FECHA_RESOLUCION = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> ParaprocesalEstatus() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_PARAPROCESALJL\n"
                   + "WHERE ESTATUS_EXPEDIENTE = 1\n"
                   + "AND (FECHA_CONCLUSION_EXPE IS NULL OR FECHA_CONCLUSION_EXPE = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> EjecucionEstatus() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_EJECUCIONJL\n"
                   + "WHERE ESTATUS_EXPE = 1\n"
                   + "AND (FECHA_CONCLUSION IS NULL OR FECHA_CONCLUSION = DATE '1899-09-09')";
        return ejecutar3Cols(sql);
    }

    // ---------------- DEMANDA ----------------
    public ArrayList<String[]> OrdinarioEstatusDemanda() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_ORDINARIOJL\n"
                   + "WHERE ESTATUS_DEMANDA <> 1\n"
                   + "AND (AUDIENCIA_PRELIM = 1 OR AUDIENCIA_JUICIO = 1)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualEstatusDemanda() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE ESTATUS_DEMANDA <> 1\n"
                   + "AND (TRAMITACION_DEPURACION = 1 OR AUDIENCIA_PRELIM = 1 OR AUDIENCIA_JUICIO = 1)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> ColectivoEstatusDemanda() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECTIVOJL\n"
                   + "WHERE ESTATUS_DEMANDA <> 1\n"
                   + "AND (AUTO_DEPURACION = 1 OR AUDIENCIA_JUICIO = 1)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> Colectivo_EconomEstatusDemanda() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECT_ECONOMJL\n"
                   + "WHERE ESTATUS_DEMANDA <> 1\n"
                   + "AND AUDIENCIA_ECONOM = 1";
        return ejecutar3Cols(sql);
    }

    // ---------------- FASE_SOLI_EXPEDIENTE (corrige IN (99)) ----------------
    public ArrayList<String[]> OrdinarioFaseSolExpFE() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_ORDINARIOJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCIONFE IS NOT NULL OR FECHA_DICTO_RESOLUCIONFE IS NOT NULL OR MONTO_SOLUCION_FE IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> OrdinarioFaseSolExpAP() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_ORDINARIOJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCIONAP IS NOT NULL OR FECHA_DICTO_RESOLUCIONAP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> OrdinarioFaseSolExpAJ() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_ORDINARIOJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCIONAJ IS NOT NULL OR FECHA_RESOLUCIONAJ IS NOT NULL OR MONTO_SOLUCIONAJ IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualFaseSolExpAP() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_AP IS NOT NULL OR FECHA_DICTO_RESOLUCION_AP IS NOT NULL OR MONTO_SOLUCION_AP IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualFaseSolExpAJ() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_AJ IS NOT NULL OR FECHA_DICTO_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualFaseSolExpAD() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> IndividualFaseSolExpTA() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_INDIVIDUALJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_TA IS NOT NULL OR FECHA_RESOLUCION_TA IS NOT NULL OR TIPO_SENTENCIA_TA IS NOT NULL OR MONTO_SOLUCION_TA IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> ColectivoFaseSolExpAJ() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECTIVOJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_AJ IS NOT NULL OR FECHA_RESOLUCION_AJ IS NOT NULL OR TIPO_SENTENCIA_AJ IS NOT NULL OR MONTO_SOLUCION_AJ IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> ColectivoFaseSolExpAD() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECTIVOJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_AD IS NOT NULL OR FECHA_DICTO_RESOLUCION_AD IS NOT NULL OR TIPO_SENTENCIA_AD IS NOT NULL OR MONTO_SOLUCION_AD IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> HuelgaFaseSolExpEmplaz() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_HUELGAJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_EMPLAZ IS NOT NULL OR FECHA_RESOLU_EMPLAZ IS NOT NULL OR INCREMENTO_SOLICITADO IS NOT NULL OR INCREMENTO_OTORGADO IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> HuelgaFaseSolExpHu() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_HUELGAJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION_HUELGA IS NOT NULL OR FECHA_RESOLU_HUELGA IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL\n"
                   + "     OR FECHA_ESTALLAM_HUELGA IS NOT NULL OR FECHA_LEVANT_HUELGA IS NOT NULL OR MONTO_ESTIPULADO IS NOT NULL OR SALARIOS_CAIDOS IS NOT NULL)";
        return ejecutar3Cols(sql);
    }

    public ArrayList<String[]> Colectivo_EconomFaseSolExpAPC() {
        String sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, COMENTARIOS\n"
                   + "FROM V3_TR_COLECT_ECONOMJL\n"
                   + "WHERE (FASE_SOLI_EXPEDIENTE IN (99) OR FASE_SOLI_EXPEDIENTE IS NULL)\n"
                   + "AND (FORMA_SOLUCION IS NOT NULL OR FECHA_RESOLUCION IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL\n"
                   + "     OR AUMENTO_PERSONAL IS NOT NULL OR DISMINUCION_PERSONAL IS NOT NULL\n"
                   + "     OR AUMENTO_JORNADALAB IS NOT NULL OR DISMINUCION_JORNADALAB IS NOT NULL\n"
                   + "     OR AUMENTO_SEMANA IS NOT NULL OR DISMINUCION_SEMANA IS NOT NULL\n"
                   + "     OR AUMENTO_SALARIOS IS NOT NULL OR DISMINUCION_SALARIOS IS NOT NULL)";
        return ejecutar3Cols(sql);
    }
}
