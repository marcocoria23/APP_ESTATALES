/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mx.org.querys.NotNull;

import Conexion.ConexionH2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ANTONIO.CORIA
 */
public class V3Huelga {

    private static final Logger LOG = Logger.getLogger(V3Huelga.class.getName());

    String sql;
    ArrayList<String[]> Array;

    // Tipo de asunto no debe ser 9 (No_identificado) o Null. (sin filtros)
    public ArrayList Tipo_Asunto() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN TIPO_ASUNTO = 9 THEN 'No_identificado' ELSE NULL END AS TIPO_ASUNTO, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE TIPO_ASUNTO = 9";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("TIPO_ASUNTO"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Tipo_Asunto()", ex);
        }

        return Array;
    }

    // Emplazamiento a huelga = No/No identificado -> no debe capturar FECHA_EMPLAZAMIENTO (sin filtros)
    public ArrayList Emplaz_huelga() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN EMPLAZAMIENTO_HUELGA = 2 THEN 'No' "
            + "  WHEN EMPLAZAMIENTO_HUELGA = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS EMPLAZAMIENTO_HUELGA, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE EMPLAZAMIENTO_HUELGA IN (2,9) "
            + "  AND FECHA_EMPLAZAMIENTO IS NOT NULL";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("EMPLAZAMIENTO_HUELGA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Emplaz_huelga()", ex);
        }

        return Array;
    }

    // Prehuelga = No/No identificado -> no debe capturar AUDIENCIA_CONCILIACION ni FECHA_AUDIENCIA (sin filtros)
    public ArrayList Preghuelga() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN PREHUELGA = 2 THEN 'No' "
            + "  WHEN PREHUELGA = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS PREHUELGA, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE PREHUELGA IN (2,9) "
            + "  AND (AUDIENCIA_CONCILIACION IS NOT NULL OR FECHA_AUDIENCIA IS NOT NULL)";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("PREHUELGA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Preghuelga()", ex);
        }

        return Array;
    }

    // Audiencia conciliación = No/No identificado y PREHUELGA=1 -> no debe capturar FECHA_AUDIENCIA (sin filtros)
    public ArrayList Aud_conciliacion() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN AUDIENCIA_CONCILIACION = 2 THEN 'No' "
            + "  WHEN AUDIENCIA_CONCILIACION = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS AUDIENCIA_CONCILIACION, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE PREHUELGA = 1 "
            + "  AND AUDIENCIA_CONCILIACION IN (2,9) "
            + "  AND FECHA_AUDIENCIA IS NOT NULL";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("AUDIENCIA_CONCILIACION")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Aud_conciliacion()", ex);
        }

        return Array;
    }

    // Estallamiento huelga = No/No identificado -> no debe capturar DECLARA_LICITUD_HUELGA / DECLARA_EXISTEN_HUELGA (sin filtros)
    public ArrayList Estallamiento_huelga() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE "
            + "  WHEN ESTALLAMIENTO_HUELGA = 2 THEN 'No' "
            + "  WHEN ESTALLAMIENTO_HUELGA = 9 THEN 'No identificado' "
            + "  ELSE NULL "
            + "END AS ESTALLAMIENTO_HUELGA, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE ESTALLAMIENTO_HUELGA IN (2,9) "
            + "  AND (DECLARA_LICITUD_HUELGA IS NOT NULL OR DECLARA_EXISTEN_HUELGA IS NOT NULL)";

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTALLAMIENTO_HUELGA")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estallamiento_huelga()", ex);
        }

        return Array;
    }

    // Estatus expediente no debe ser 9 (sin filtros)
    public ArrayList Estatus_ExpedienteNI() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN ESTATUS_EXPEDIENTE = 9 THEN 'No_identificado' ELSE NULL END AS ESTATUS_EXPEDIENTE, "
            + "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE ESTATUS_EXPEDIENTE = 9";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("COMENTARIOS"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_ExpedienteNI()", ex);
        }

        return Array;
    }

    // Incompetencia no debe ser 9 (sin filtros)
    public ArrayList IncompetenciaNI() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, "
            + "REPLACE(COMENTARIOS, ',', '') AS COMENTARIOS, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE INCOMPETENCIA = 9";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("COMENTARIOS"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en IncompetenciaNI()", ex);
        }

        return Array;
    }

    // Incompetencia=2 (No) y TIPO_INCOMPETENCIA no debe venir (sin filtros)
    public ArrayList Tipo_IncompetenciaNI() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN INCOMPETENCIA = 2 THEN 'No' ELSE NULL END AS INCOMPETENCIA, "
            + "TIPO_INCOMPETENCIA, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE INCOMPETENCIA = 2 "
            + "  AND TIPO_INCOMPETENCIA IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Tipo_IncompetenciaNI()", ex);
        }

        return Array;
    }

    // Incompetencia=1 y hay datos contestados después (sin filtros)
    public ArrayList PivIncompetencia() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE INCOMPETENCIA = 1 "
            + "  AND ("
            + "    FECHA_PRESENTA_PETIC IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR "
            + "    EMPLAZAMIENTO_HUELGA IS NOT NULL OR FECHA_EMPLAZAMIENTO IS NOT NULL OR PREHUELGA IS NOT NULL OR "
            + "    AUDIENCIA_CONCILIACION IS NOT NULL OR FECHA_AUDIENCIA IS NOT NULL OR ESTALLAMIENTO_HUELGA IS NOT NULL OR "
            + "    DECLARA_LICITUD_HUELGA IS NOT NULL OR DECLARA_EXISTEN_HUELGA IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR "
            + "    FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION_EMPLAZ IS NOT NULL OR "
            + "    ESPECIFI_FORMA_EMPLAZ IS NOT NULL OR FECHA_RESOLU_EMPLAZ IS NOT NULL OR INCREMENTO_SOLICITADO IS NOT NULL OR "
            + "    INCREMENTO_OTORGADO IS NOT NULL OR FORMA_SOLUCION_HUELGA IS NOT NULL OR ESPECIFI_FORMA_HUELGA IS NOT NULL OR "
            + "    FECHA_RESOLU_HUELGA IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL OR FECHA_ESTALLAM_HUELGA IS NOT NULL OR "
            + "    FECHA_LEVANT_HUELGA IS NOT NULL OR MONTO_ESTIPULADO IS NOT NULL OR SALARIOS_CAIDOS IS NOT NULL"
            + "  )";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en PivIncompetencia()", ex);
        }

        return Array;
    }

    // Incompetencia=9 y hay datos contestados (sin filtros)
    public ArrayList PivIncompetencia_Noidentificado() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, INCOMPETENCIA, PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE INCOMPETENCIA = 9 "
            + "  AND ("
            + "    FECHA_PRESENTA_PETIC IS NOT NULL OR CANTIDAD_ACTORES IS NOT NULL OR CANTIDAD_DEMANDADOS IS NOT NULL OR "
            + "    EMPLAZAMIENTO_HUELGA IS NOT NULL OR FECHA_EMPLAZAMIENTO IS NOT NULL OR PREHUELGA IS NOT NULL OR "
            + "    AUDIENCIA_CONCILIACION IS NOT NULL OR FECHA_AUDIENCIA IS NOT NULL OR ESTALLAMIENTO_HUELGA IS NOT NULL OR "
            + "    DECLARA_LICITUD_HUELGA IS NOT NULL OR DECLARA_EXISTEN_HUELGA IS NOT NULL OR ESTATUS_EXPEDIENTE IS NOT NULL OR "
            + "    FECHA_ACTO_PROCESAL IS NOT NULL OR FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION_EMPLAZ IS NOT NULL OR "
            + "    ESPECIFI_FORMA_EMPLAZ IS NOT NULL OR FECHA_RESOLU_EMPLAZ IS NOT NULL OR INCREMENTO_SOLICITADO IS NOT NULL OR "
            + "    INCREMENTO_OTORGADO IS NOT NULL OR FORMA_SOLUCION_HUELGA IS NOT NULL OR ESPECIFI_FORMA_HUELGA IS NOT NULL OR "
            + "    FECHA_RESOLU_HUELGA IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL OR FECHA_ESTALLAM_HUELGA IS NOT NULL OR "
            + "    FECHA_LEVANT_HUELGA IS NOT NULL OR MONTO_ESTIPULADO IS NOT NULL OR SALARIOS_CAIDOS IS NOT NULL"
            + "  )";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("INCOMPETENCIA"),
                    rs.getString("PERIODO")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en PivIncompetencia_Noidentificado()", ex);
        }

        return Array;
    }

    // Estatus expediente=2 pero hay datos de solución contestados (sin filtros)
    public ArrayList Estatus_Expediente() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE ESTATUS_EXPEDIENTE = 2 "
            + "  AND ("
            + "    FASE_SOLI_EXPEDIENTE IS NOT NULL OR FORMA_SOLUCION_EMPLAZ IS NOT NULL OR "
            + "    ESPECIFI_FORMA_EMPLAZ IS NOT NULL OR FECHA_RESOLU_EMPLAZ IS NOT NULL OR INCREMENTO_SOLICITADO IS NOT NULL OR "
            + "    INCREMENTO_OTORGADO IS NOT NULL OR FORMA_SOLUCION_HUELGA IS NOT NULL OR ESPECIFI_FORMA_HUELGA IS NOT NULL OR "
            + "    FECHA_RESOLU_HUELGA IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL OR FECHA_ESTALLAM_HUELGA IS NOT NULL OR "
            + "    FECHA_LEVANT_HUELGA IS NOT NULL OR MONTO_ESTIPULADO IS NOT NULL OR SALARIOS_CAIDOS IS NOT NULL"
            + "  )";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Estatus_Expediente()", ex);
        }

        return Array;
    }

    // Estatus expediente=1 y FECHA_ACTO_PROCESAL no debe venir (sin filtros)
    public ArrayList Fecha_acto_procesal() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE ESTATUS_EXPEDIENTE = 1 "
            + "  AND FECHA_ACTO_PROCESAL IS NOT NULL";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("ESTATUS_EXPEDIENTE"),
                    rs.getString("FECHA_ACTO_PROCESAL")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fecha_acto_procesal()", ex);
        }

        return Array;
    }

    // FASE_SOLI_EXPEDIENTE "no existe" (según tu regla original). Sin filtros.
    // Nota: tu SQL original hace JOIN con coma y luego "NOT IN", aquí lo dejo igual pero sin el filtro por entidad/periodo.
    public ArrayList Fase_Sol_expNoExiste() {
        Array = new ArrayList();

        sql = "SELECT P.CLAVE_ORGANO, P.EXPEDIENTE_CLAVE, S.DESCRIPCION AS FASE_SOLI_EXPEDIENTE "
            + "FROM V3_TR_HUELGAJL P, V3_TC_FASE_EXPEDIENTEJL S "
            + "WHERE P.FASE_SOLI_EXPEDIENTE = S.ID "
            + "  AND P.FASE_SOLI_EXPEDIENTE NOT IN (5,6,7,99)";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_expNoExiste()", ex);
        }

        return Array;
    }

    // FASE_SOLI_EXPEDIENTE = 99 y hay datos contestados de solución (sin filtros)
    public ArrayList Fase_Sol_expNI() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "CASE WHEN FASE_SOLI_EXPEDIENTE = 99 THEN 'No identificado' ELSE NULL END AS FASE_SOLI_EXPEDIENTE, "
            + "PERIODO "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE FASE_SOLI_EXPEDIENTE = 99 "
            + "  AND ("
            + "    FORMA_SOLUCION_EMPLAZ IS NOT NULL OR ESPECIFI_FORMA_EMPLAZ IS NOT NULL OR FECHA_RESOLU_EMPLAZ IS NOT NULL OR "
            + "    INCREMENTO_SOLICITADO IS NOT NULL OR INCREMENTO_OTORGADO IS NOT NULL OR FORMA_SOLUCION_HUELGA IS NOT NULL OR "
            + "    ESPECIFI_FORMA_HUELGA IS NOT NULL OR FECHA_RESOLU_HUELGA IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL OR "
            + "    FECHA_ESTALLAM_HUELGA IS NOT NULL OR FECHA_LEVANT_HUELGA IS NOT NULL OR MONTO_ESTIPULADO IS NOT NULL OR "
            + "    SALARIOS_CAIDOS IS NOT NULL"
            + "  )";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_expNI()", ex);
        }

        return Array;
    }

    // FASE_SOLI_EXPEDIENTE en (5,6) pero hay datos de "Solución (Huelga)" (sin filtros)
    public ArrayList Fase_Sol_exp_EMPH() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE FASE_SOLI_EXPEDIENTE IN (5,6) "
            + "  AND ("
            + "    FORMA_SOLUCION_HUELGA IS NOT NULL OR ESPECIFI_FORMA_HUELGA IS NOT NULL OR "
            + "    FECHA_RESOLU_HUELGA IS NOT NULL OR TIPO_SENTENCIA IS NOT NULL OR FECHA_ESTALLAM_HUELGA IS NOT NULL OR "
            + "    FECHA_LEVANT_HUELGA IS NOT NULL OR MONTO_ESTIPULADO IS NOT NULL OR SALARIOS_CAIDOS IS NOT NULL"
            + "  )";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_exp_EMPH()", ex);
        }

        return Array;
    }

    // FASE_SOLI_EXPEDIENTE = 7 pero hay datos de "Solución (Emplazamiento/Prehuelga)" (sin filtros)
    public ArrayList Fase_Sol_exp_Huelga() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, FASE_SOLI_EXPEDIENTE "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE FASE_SOLI_EXPEDIENTE = 7 "
            + "  AND ("
            + "    FORMA_SOLUCION_EMPLAZ IS NOT NULL OR ESPECIFI_FORMA_EMPLAZ IS NOT NULL OR "
            + "    FECHA_RESOLU_EMPLAZ IS NOT NULL OR INCREMENTO_SOLICITADO IS NOT NULL OR INCREMENTO_OTORGADO IS NOT NULL"
            + "  )";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FASE_SOLI_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en Fase_Sol_exp_Huelga()", ex);
        }

        return Array;
    }

    // Sin motivo de conflicto (sin filtros). OJO: en Oracle usabas TO_CHAR, aquí uso FORMATDATETIME para H2.
    public ArrayList SinMotivo_Conflicto() {
        Array = new ArrayList();

        sql = "SELECT CLAVE_ORGANO, EXPEDIENTE_CLAVE, "
            + "FORMATDATETIME(FECHA_APERTURA_EXPEDIENTE, 'dd/MM/yyyy') AS FECHA_APERTURA_EXPEDIENTE "
            + "FROM V3_TR_HUELGAJL "
            + "WHERE firma_contrato = 2 "
            + "  AND revision_contrato = 2 "
            + "  AND incumplim_contrato = 2 "
            + "  AND revision_salario = 2 "
            + "  AND reparto_utilidades = 2 "
            + "  AND apoyo_otra_huelga = 2 "
            + "  AND desequilibrio_fac_prod = 2 "
            + "  AND otro_motivo = 2";

        System.out.println(sql);

        try (Connection con = ConexionH2.getConnection();
             PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Array.add(new String[]{
                    rs.getString("CLAVE_ORGANO"),
                    rs.getString("EXPEDIENTE_CLAVE"),
                    rs.getString("FECHA_APERTURA_EXPEDIENTE")
                });
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Error en SinMotivo_Conflicto()", ex);
        }

        return Array;
    }

    /*
     * NOTA IMPORTANTE:
     * El método Fase_Sol_Desechamiento() que pegaste trae varias tablas TC_RALAB_* y V3_TC_*,
     * y usa DECODE + concatenaciones + comparaciones por DESCRIPCION.
     * Si también lo quieres en H2 "sin filtros", dímelo y lo convierto,
     * pero ahí sí hay que revisar nombres reales de tablas en tu H2 (porque TC_RALAB_* quizá no existan).
     */
    
    //---Cuando el expediente No se admitio a tramite la fase de solicitud debe ser emplazamiento a huelga o prehuelga.
// (convertido a H2, sin filtros, sin DECODE, usando DISTINCT)
public ArrayList Fase_Sol_Desechamiento() {
    Array = new ArrayList();

    sql =
        "SELECT DISTINCT " +
        "  (P1.CLAVE_ORGANO || P1.EXPEDIENTE_CLAVE || P1.PERIODO) AS UNIQUE_CODE, " +
        "  P1.CLAVE_ORGANO, " +
        "  P1.EXPEDIENTE_CLAVE, " +
        "  CASE P1.FASE_SOLI_EXPEDIENTE " +
        "    WHEN 1  THEN 'Audiencia preliminar' " +
        "    WHEN 2  THEN 'Audiencia de juicio' " +
        "    WHEN 3  THEN 'Tramitación por auto de depuración' " +
        "    WHEN 4  THEN 'Tramitación sin audiencias' " +
        "    WHEN 5  THEN 'Emplazamiento a huelga' " +
        "    WHEN 6  THEN 'Prehuelga' " +
        "    WHEN 7  THEN 'Huelga' " +
        "    WHEN 8  THEN 'Audiencia dentro del procedimiento colectivo de naturaleza económica' " +
        "    WHEN 9  THEN 'Fase escrita' " +
        "    WHEN 99 THEN 'No identificado' " +
        "    ELSE NULL " +
        "  END AS FASE_SOLI_EXPEDIENTE, " +
        "  CASE P1.FORMA_SOLUCION_HUELGA " +
        "    WHEN 1 THEN 'Sentencia' " +
        "    WHEN 2 THEN 'Convenio conciliatorio' " +
        "    WHEN 3 THEN 'Desistimiento' " +
        "    WHEN 4 THEN 'Caducidad' " +
        "    WHEN 5 THEN 'Otra forma de solución (especifique)' " +
        "    WHEN 9 THEN 'No Identificado' " +
        "    ELSE NULL " +
        "  END AS FORMA_SOLUCION_HUELGA, " +
        "  P1.ESPECIFI_FORMA_HUELGA, " +
        "  P1.COMENTARIOS, " +
        "  P1.PERIODO " +
        "FROM " +
        "  V3_TR_HUELGAJL P1, " +
        "  TC_RALAB_FASEJL P2, " +
        "  TC_RALAB_FORMA_SOLUCIONJL P3, " +
        "  TC_RALAB_ESPECIFIQUEJL P4, " +
        "  TC_RALAB_COMENTARIOSJL P5 " +
        "WHERE " +
        "  ( " +
        "    TRIM(UPPER( " +
        "      CASE P1.FASE_SOLI_EXPEDIENTE " +
        "        WHEN 1  THEN 'Audiencia preliminar' " +
        "        WHEN 2  THEN 'Audiencia de juicio' " +
        "        WHEN 3  THEN 'Tramitación por auto de depuración' " +
        "        WHEN 4  THEN 'Tramitación sin audiencias' " +
        "        WHEN 5  THEN 'Emplazamiento a huelga' " +
        "        WHEN 6  THEN 'Prehuelga' " +
        "        WHEN 7  THEN 'Huelga' " +
        "        WHEN 8  THEN 'Audiencia dentro del procedimiento colectivo de naturaleza económica' " +
        "        WHEN 9  THEN 'Fase escrita' " +
        "        WHEN 99 THEN 'No identificado' " +
        "        ELSE NULL " +
        "      END " +
        "    )) = TRIM(UPPER(P2.DESCRIPCION)) " +
        "    AND " +
        "    TRIM(UPPER( " +
        "      CASE P1.FORMA_SOLUCION_HUELGA " +
        "        WHEN 1 THEN 'Sentencia' " +
        "        WHEN 2 THEN 'Convenio conciliatorio' " +
        "        WHEN 3 THEN 'Desistimiento' " +
        "        WHEN 4 THEN 'Caducidad' " +
        "        WHEN 5 THEN 'Otra forma de solución (especifique)' " +
        "        WHEN 9 THEN 'No Identificado' " +
        "        ELSE NULL " +
        "      END " +
        "    )) = TRIM(UPPER(P3.DESCRIPCION)) " +
        "    AND " +
        "    ( (P1.ESPECIFI_FORMA_HUELGA IS NOT NULL AND TRIM(UPPER(P1.ESPECIFI_FORMA_HUELGA)) LIKE TRIM(UPPER(P4.DESCRIPCION))) " +
        "      OR (P1.COMENTARIOS IS NOT NULL AND TRIM(UPPER(P1.COMENTARIOS)) LIKE TRIM(UPPER(P5.DESCRIPCION))) " +
        "    ) " +
        "  )";

    System.out.println(sql);

    try (java.sql.Connection con = ConexionH2.getConnection();
         java.sql.PreparedStatement ps = con.prepareStatement(sql);
         java.sql.ResultSet rs = ps.executeQuery()) {

        while (rs.next()) {
            Array.add(new String[]{
                rs.getString("CLAVE_ORGANO"),
                rs.getString("EXPEDIENTE_CLAVE"),
                rs.getString("FASE_SOLI_EXPEDIENTE"),
                rs.getString("FORMA_SOLUCION_HUELGA"),
                rs.getString("ESPECIFI_FORMA_HUELGA"),
                rs.getString("COMENTARIOS")
            });
        }

    } catch (SQLException ex) {
        Logger.getLogger(V3Huelga.class.getName()).log(Level.SEVERE, "Error en Fase_Sol_Desechamiento()", ex);
    }

    return Array;
}

}
