/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
CUANDO EL PROGRAMA SE QUEDA EN ST.EXECUTE POR CADA PROCEDIMIENTO FAVOR DE REVISAR DOCUMENTO YA QUE PUEDE SER LA TABLA
 TMP POR EL RAISE.APLICATION Y POR QUE LOS CAMPOS FECHA EN ,CSV NO ESTAN EN FORMATO FECHA
 */
package mx.org.Insert.BD;

import Pantallas_laborales.InsertaTR;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import Bean_Procedures.Huelga;
import ConverCat.Convers;
import Pantallas_laborales.cargando;
import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;

/**
 *
 * @author ANTONIO.CORIA
 */
public class ReadCSV_Huelga {

    public static String impErro = "", RutaT = "", CampoNotConver = "";
    public static int TotalRegistros = 0;
    public static boolean borra_ruta = false;
    ArrayList Array;
    public static String rutaCarpetaArchivos = "";
    Convertir_utf8 conUTF8 = new Convertir_utf8();

    public void Read_Huelga(Connection con, Connection conErr) throws FileNotFoundException, IOException {
        InsertaTR Insert = new InsertaTR();
        //FileInputStream f = new FileInputStream(Insert.rutaT);  
        try {
            System.out.println("1.....");
            if (Insert.CarpetaArchivos == true) {
                System.out.println("2.....");
                rutaCarpetaArchivos = Insert.rutaT + "CSV_BD_T.4.1_huelga.csv";
                System.out.println("+++++2" + rutaCarpetaArchivos);
            } else {
                System.out.println("3.....");
                rutaCarpetaArchivos = Insert.rutaT;
                System.out.println("+++++3" + rutaCarpetaArchivos);
            }
            if (Insert.Bandera == false) {
                System.out.println("4.....");
                try ( BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(rutaCarpetaArchivos))) {
                    byte[] bytes = new byte[3];
                    int bytesRead = inputStream.read(bytes);

                    if (bytesRead >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                        System.out.println("Archivo en UTF-8");
                        System.out.println("5.....");
                        IN_HUELGA(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("6.....");
                        conUTF8.Convertir_utf8_EBaseDatos(rutaCarpetaArchivos);
                        IN_HUELGA(conUTF8.rutaNuevoArchivo, con, conErr);
                        rutaCarpetaArchivos = conUTF8.rutaNuevoArchivo;

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("7.....");
                try ( BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(rutaCarpetaArchivos))) {
                    byte[] bytes = new byte[3];
                    int bytesRead = inputStream.read(bytes);

                    if (bytesRead >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                        System.out.println("8.....");
                        System.out.println("Archivo en UTF-8");
                        IN_HUELGA(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("9.....");
                        conUTF8.Convertir_utf8(rutaCarpetaArchivos);
                        IN_HUELGA(conUTF8.rutaNuevoArchivo, con, conErr);
                        rutaCarpetaArchivos = conUTF8.rutaNuevoArchivo;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("++++++" + rutaCarpetaArchivos);
            }
        } catch (Exception ex) {
        }

    }

    public void IN_HUELGA(String Ruta, Connection con, Connection conErr) throws Exception {
        String rutaArchivoCSV = Ruta;
        Array = new ArrayList();
        TotalRegistros = 0;
        boolean Inserta = true;
        Convers conver = new Convers();
        java.sql.PreparedStatement ps = null;

        try ( BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(Ruta))) {
            byte[] bytes = new byte[3];
            int bytesRead = inputStream.read(bytes);

            if (bytesRead >= 3 && bytes[0] == (byte) 0xEF && bytes[1] == (byte) 0xBB && bytes[2] == (byte) 0xBF) {
                System.out.println("El archivo parece estar en UTF-8.");
                try ( BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(rutaArchivoCSV), StandardCharsets.UTF_8));  CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
                    int numeroColumnas = 0;
                    CSVRecord firstRecord = csvParser.iterator().next();
                    numeroColumnas = firstRecord.size();
                    System.out.println("numcol" + numeroColumnas);
                    if (numeroColumnas == 54) {
                        System.out.println("+hellooou+" + numeroColumnas);
                        cargando cargar = new cargando();
                        ArrayList<Huelga> ad = new ArrayList<>();
                        for (CSVRecord record : csvParser) {
                            // System.out.println("llenado de csv");
                            TotalRegistros++;
                            Huelga c = new Huelga();
                            c.SetNOMBRE_ORGANO_JURIS(record.get(0).toUpperCase());
                            c.SetCLAVE_ORGANO(record.get(1).toUpperCase());
                            c.SetEXPEDIENTE_CLAVE(record.get(2).toUpperCase().replace("\\n", "").trim());
                            c.SetFECHA_APERTURA_EXPEDIENTE(conver.toH2Date(record.get(3).toUpperCase(), "FECHA_APERTURA_EXPEDIENTE"));
                            c.SetTIPO_ASUNTO(conver.CON_V3_TC_TIPO_ASUNTOJL(con, record.get(4).toUpperCase()));
                            c.SetRAMA_INDUS_INVOLUCRAD(record.get(5).toUpperCase());
                            c.SetSECTOR_RAMA(conver.CON_V3_TC_SECTOR_RAMAJL(con, record.get(6).toUpperCase()));
                            c.SetSUBSECTOR_RAMA(conver.CON_V3_TC_SUBSECTOR_RAMAJL(con, record.get(7).toUpperCase()));
                            c.SetENTIDAD_CLAVE(record.get(8).toUpperCase());
                            c.SetENTIDAD_NOMBRE(record.get(9).toUpperCase());
                            c.SetMUNICIPIO_CLAVE(record.get(10).toUpperCase());
                            if (!record.get(10).toUpperCase().equals("")) {
                                if (record.get(10).toUpperCase().length() == 1) {
                                    c.SetMUNICIPIO_CLAVE(record.get(8).toUpperCase() + "00" + record.get(10).toUpperCase());
                                }
                                if (record.get(10).toUpperCase().length() == 2) {
                                    c.SetMUNICIPIO_CLAVE(record.get(8).toUpperCase() + "0" + record.get(10).toUpperCase());
                                }
                                if (record.get(10).toUpperCase().length() == 5) {
                                     c.SetMUNICIPIO_CLAVE(record.get(10).toUpperCase());
                                }
                                   if (record.get(10).toUpperCase().length() == 3) {
                                      c.SetMUNICIPIO_CLAVE(record.get(8).toUpperCase()+record.get(10).toUpperCase());
                                } 
                                
                            } else {
                                c.SetMUNICIPIO_CLAVE(record.get(10).toUpperCase());
                            }
                            c.SetMUNICIPIO_NOMBRE(record.get(11).toUpperCase());
                            c.SetFIRMA_CONTRATO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(12).toUpperCase()));
                            c.SetREVISION_CONTRATO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(13).toUpperCase()));
                            c.SetINCUMPLIM_CONTRATO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(14).toUpperCase()));
                            c.SetREVISION_SALARIO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(15).toUpperCase()));
                            c.SetREPARTO_UTILIDADES(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(16).toUpperCase()));
                            c.SetAPOYO_OTRA_HUELGA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(17).toUpperCase()));
                            c.SetDESEQUILIBRIO_FAC_PROD(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(18).toUpperCase()));
                            c.SetOTRO_MOTIVO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(19).toUpperCase()));
                            c.SetESPECIFIQUE_MOTIVO(record.get(20).toUpperCase());
                            c.SetINCOMPETENCIA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(21).toUpperCase()));
                            c.SetTIPO_INCOMPETENCIA(conver.CON_V3_TC_TIPO_INCOMPETENCIAJL(con, record.get(22).toUpperCase()));
                            c.SetESPECIFIQUE_INCOMP(record.get(23).toUpperCase());
                            c.SetFECHA_PRESENTA_PETIC(conver.toH2Date(record.get(24).toUpperCase(), "FECHA_PRESENTA_PETIC"));
                            c.SetCANTIDAD_ACTORES(record.get(25).toUpperCase());
                            c.SetCANTIDAD_DEMANDADOS(record.get(26).toUpperCase());
                            c.SetEMPLAZAMIENTO_HUELGA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(27).toUpperCase()));
                            c.SetFECHA_EMPLAZAMIENTO(conver.toH2Date(record.get(28).toUpperCase(), "FECHA_EMPLAZAMIENTO"));
                            c.SetPREHUELGA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(29).toUpperCase()));
                            c.SetAUDIENCIA_CONCILIACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(30).toUpperCase()));
                            c.SetFECHA_AUDIENCIA(conver.toH2Date(record.get(31).toUpperCase(), "FECHA_AUDIENCIA"));
                            c.SetESTALLAMIENTO_HUELGA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(32).toUpperCase()));
                            c.SetDECLARA_LICITUD_HUELGA(conver.CON_V3_TC_LICITUD_HUELGAJL(con, record.get(33).toUpperCase()));
                            c.SetDECLARA_EXISTEN_HUELGA(conver.CON_V3_TC_EXISTENCIA_HUELGAJL(con, record.get(34).toUpperCase()));
                            c.SetESTATUS_EXPEDIENTE(conver.CON_V3_TC_ESTATUS_EXPEDIENTEJL(con, record.get(35).toUpperCase()));
                            c.SetFECHA_ACTO_PROCESAL(conver.toH2Date(record.get(36).toUpperCase(), "FECHA_ACTO_PROCESAL"));
                            c.SetFASE_SOLI_EXPEDIENTE(conver.CON_V3_TC_FASE_EXPEDIENTEJL(con, record.get(37).toUpperCase()));
                            c.SetFORMA_SOLUCION_EMPLAZ(conver.CON_V3_TC_FORMA_SOLUCION_PHJL(con, record.get(38).toUpperCase()));
                            c.SetESPECIFI_FORMA_EMPLAZ(record.get(39).toUpperCase());
                            c.SetFECHA_RESOLU_EMPLAZ(conver.toH2Date(record.get(40).toUpperCase(), "FECHA_RESOLU_EMPLAZ"));
                            c.SetINCREMENTO_SOLICITADO(record.get(41).toUpperCase());
                            c.SetINCREMENTO_OTORGADO(record.get(42).toUpperCase());
                            c.SetFORMA_SOLUCION_HUELGA(conver.CON_V3_TC_FORMA_SOLUCION_HJL(con, record.get(43).toUpperCase()));
                            c.SetESPECIFI_FORMA_HUELGA(record.get(44).toUpperCase());
                            c.SetFECHA_RESOLU_HUELGA(conver.toH2Date(record.get(45).toUpperCase(), "FECHA_RESOLU_HUELGA"));
                            c.SetTIPO_SENTENCIA(conver.CON_V3_TC_TIPO_SENTENCIAJL(con, record.get(46).toUpperCase()));
                            c.SetFECHA_ESTALLAM_HUELGA(conver.toH2Date(record.get(47).toUpperCase(), "FECHA_ESTALLAM_HUELGA"));
                            c.SetFECHA_LEVANT_HUELGA(conver.toH2Date(record.get(48).toUpperCase(), "FECHA_LEVANT_HUELGA"));
                            c.SetDIAS_HUELGA(record.get(49).toUpperCase());
                            c.SetMONTO_ESTIPULADO(record.get(50).toUpperCase());
                            c.SetSALARIOS_CAIDOS(record.get(51).toUpperCase());
                            c.SetCOMENTARIOS(record.get(52).toUpperCase());
                            ad.add(c);
                        }
                        System.out.println("entro 1");
                        if (TotalRegistros > 0) {
                            if (Inserta == true) {
                                System.out.println("entro 2");
                                cargar.setVisible(true);
                                // OJO: AJUSTA los nombres reales de columnas en tu tabla H2
                                final String sql
                                        = "INSERT INTO V3_TR_HUELGAJL ("
                                        + "NOMBRE_ORGANO_JURIS, CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, TIPO_ASUNTO,\n"
                                        + "RAMA_INDUS_INVOLUCRAD, SECTOR_RAMA, SUBSECTOR_RAMA, ENTIDAD_CLAVE, ENTIDAD_NOMBRE,\n"
                                        + "MUNICIPIO_CLAVE, MUNICIPIO_NOMBRE, FIRMA_CONTRATO, REVISION_CONTRATO, INCUMPLIM_CONTRATO,\n"
                                        + "REVISION_SALARIO, REPARTO_UTILIDADES, APOYO_OTRA_HUELGA, DESEQUILIBRIO_FAC_PROD, OTRO_MOTIVO,\n"
                                        + "ESPECIFIQUE_MOTIVO, INCOMPETENCIA, TIPO_INCOMPETENCIA, ESPECIFIQUE_INCOMP, FECHA_PRESENTA_PETIC,\n"
                                        + "CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS, EMPLAZAMIENTO_HUELGA, FECHA_EMPLAZAMIENTO, PREHUELGA,\n"
                                        + "AUDIENCIA_CONCILIACION, FECHA_AUDIENCIA, ESTALLAMIENTO_HUELGA, DECLARA_LICITUD_HUELGA, DECLARA_EXISTEN_HUELGA,\n"
                                        + "ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL, FASE_SOLI_EXPEDIENTE, FORMA_SOLUCION_EMPLAZ, ESPECIFI_FORMA_EMPLAZ,\n"
                                        + "FECHA_RESOLU_EMPLAZ, INCREMENTO_SOLICITADO, INCREMENTO_OTORGADO, FORMA_SOLUCION_HUELGA, ESPECIFI_FORMA_HUELGA,\n"
                                        + "FECHA_RESOLU_HUELGA, TIPO_SENTENCIA, FECHA_ESTALLAM_HUELGA, FECHA_LEVANT_HUELGA, DIAS_HUELGA,\n"
                                        + "MONTO_ESTIPULADO, SALARIOS_CAIDOS, COMENTARIOS"
                                        + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                                try {
                                    System.out.println("A) antes getConnection");
                                    System.out.println("B) despues getConnection");
                                    // ✅ Conexión separada para guardar ERRORES_INSERT (NO se revierte con rollback)
                                    conErr.setAutoCommit(true);
                                    con.setAutoCommit(false);
                                    System.out.println("C) antes prepareStatement");
                                    ps = con.prepareStatement(sql);
                                    System.out.println("D) despues prepareStatement");
                                    int batch = 0;
                                    int blockStart = 0; // índice inicial del bloque actual dentro de "ad"
                                    System.out.println("tamaño del array=" + ad.size());
                                    for (int i = 0; i < ad.size(); i++) {
                                        Huelga a = ad.get(i);
                                        // ✅ usa tu helper para setear params (mismo orden de tu INSERT)
                                        setParamsHuelga(ps, a);
                                        ps.addBatch();
                                        batch++;
                                        // Ejecutamos cada 1000
                                        if (batch % 1000 == 0) {
                                            int blockEnd = i + 1; // fin exclusivo
                                            try {
                                                ps.executeBatch();
                                                con.commit();
                                                System.out.println("✅ batch OK (bloque " + blockStart + " - " + blockEnd + ")");
                                                blockStart = blockEnd;
                                            } catch (BatchUpdateException bue) {
                                                System.err.println("❌ Batch falló (bloque " + blockStart + " - " + blockEnd + ")");
                                                System.err.println("Mensaje: " + bue.getMessage());
                                                con.rollback();

                                                // ✅ fallback: inserta uno por uno y guarda en ERRORES_INSERT lo que falle
                                                insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, blockEnd, "V3_TR_HUELGAJL");

                                                // ✅ commit de los buenos (los que sí entraron en el uno-a-uno)
                                                con.commit();
                                                blockStart = blockEnd;
                                            }
                                        }
                                    }
                                    // ✅ Último bloque (si no fue múltiplo de 1000)
                                    if (blockStart < ad.size()) {
                                        try {
                                            ps.executeBatch();
                                            con.commit();
                                            System.out.println("✅ último batch OK (bloque " + blockStart + " - " + ad.size() + ")");
                                        } catch (BatchUpdateException bue) {
                                            System.err.println("❌ Último batch falló (bloque " + blockStart + " - " + ad.size() + ")");
                                            System.err.println("Mensaje: " + bue.getMessage());
                                            con.rollback();
                                            insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, ad.size(), "V3_TR_HUELGAJL");
                                            con.commit();
                                        }
                                    }
                                    System.out.println("✅ Inserción terminada. Errores guardados en ERRORES_INSERT (si hubo).");
                                } catch (SQLException e) {
                                    System.out.println("error" + e);
                                    if (con != null) {
                                        con.rollback();
                                    }
                                    System.err.println("❌ SQLException");
                                    System.err.println("Mensaje: " + e.getMessage());
                                    System.err.println("SQLState: " + e.getSQLState());
                                    System.err.println("ErrorCode: " + e.getErrorCode());
                                    e.printStackTrace();
                                } catch (Exception e) {
                                    if (con != null) try {
                                        con.rollback();
                                    } catch (SQLException ex) {
                                    }
                                    System.err.println("❌ ERROR JAVA");
                                    e.printStackTrace();
                                } finally {
                                    if (ps != null) try {
                                        ps.close();
                                        System.out.println("parametros cerrados");
                                    } catch (SQLException ex) {
                                    }
                                    cargar.setVisible(false);
                                }
                            } else {
                            }

                        } else {
                            JOptionPane.showMessageDialog(null, "Archivo .CSV sin Registros-V3_TMP_HUELGAJL");
                        }
                    } else {
                        JOptionPane.showMessageDialog(null, "Numero de columnas no coincide con la Base de datos");
                    }
                }
            } else {
                JOptionPane.showMessageDialog(null, "Gormato de archivo incorrecto");
            }

        }
    }

    private static final String SQL_INSERT_ERROR
            = "INSERT INTO ERRORES_INSERT "
            + "(TABLA_DESTINO, CLAVE_ORGANO, EXPEDIENTE_CLAVE, ID, "
            + " SQLSTATE, ERRORCODE, MENSAJE, REGISTRO_RAW) "
            + "VALUES (?,?,?,?,?,?,?,?)";

    private static void guardarError(Connection conErr, String tablaDestino, Huelga a,
            SQLException e, String raw) {
        try ( PreparedStatement pe = conErr.prepareStatement(SQL_INSERT_ERROR)) {
            pe.setString(1, tablaDestino);
            pe.setString(2, a.GetCLAVE_ORGANO());
            pe.setString(3, a.GetEXPEDIENTE_CLAVE());
            pe.setString(4, "");
            pe.setString(5, e.getSQLState());
            pe.setInt(6, e.getErrorCode());
            String msg = e.getMessage().replace("Violación de indice de Unicidad ó Clave primaria", "Registro Duplicado").replace("Violación de una restricción de Integridad Referencial", "Valor de Catalogo no encontrado");
            pe.setString(7, msg != null && msg.length() > 500 ? msg.substring(0, 250) : msg);
            pe.setString(8, raw);
            pe.executeUpdate();
        } catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
    }

    private static void setParamsHuelga(PreparedStatement ps, Huelga a) throws SQLException {
        ps.setString(1, a.GetNOMBRE_ORGANO_JURIS());
        ps.setString(2, a.GetCLAVE_ORGANO());
        ps.setString(3, a.GetEXPEDIENTE_CLAVE());
        ps.setString(4, a.GetFECHA_APERTURA_EXPEDIENTE());
        ps.setString(5, a.GetTIPO_ASUNTO());

        ps.setString(6, a.GetRAMA_INDUS_INVOLUCRAD());
        ps.setString(7, a.GetSECTOR_RAMA());
        ps.setString(8, a.GetSUBSECTOR_RAMA());
        ps.setString(9, a.GetENTIDAD_CLAVE());
        ps.setString(10, a.GetENTIDAD_NOMBRE());

        ps.setString(11, a.GetMUNICIPIO_CLAVE());
        ps.setString(12, a.GetMUNICIPIO_NOMBRE());
        ps.setString(13, a.GetFIRMA_CONTRATO());
        ps.setString(14, a.GetREVISION_CONTRATO());
        ps.setString(15, a.GetINCUMPLIM_CONTRATO());

        ps.setString(16, a.GetREVISION_SALARIO());
        ps.setString(17, a.GetREPARTO_UTILIDADES());
        ps.setString(18, a.GetAPOYO_OTRA_HUELGA());
        ps.setString(19, a.GetDESEQUILIBRIO_FAC_PROD());
        ps.setString(20, a.GetOTRO_MOTIVO());

        ps.setString(21, a.GetESPECIFIQUE_MOTIVO());
        ps.setString(22, a.GetINCOMPETENCIA());
        ps.setString(23, a.GetTIPO_INCOMPETENCIA());
        ps.setString(24, a.GetESPECIFIQUE_INCOMP());
        ps.setString(25, a.GetFECHA_PRESENTA_PETIC());

        ps.setString(26, a.GetCANTIDAD_ACTORES());
        ps.setString(27, a.GetCANTIDAD_DEMANDADOS());
        ps.setString(28, a.GetEMPLAZAMIENTO_HUELGA());
        ps.setString(29, a.GetFECHA_EMPLAZAMIENTO());
        ps.setString(30, a.GetPREHUELGA());

        ps.setString(31, a.GetAUDIENCIA_CONCILIACION());
        ps.setString(32, a.GetFECHA_AUDIENCIA());
        ps.setString(33, a.GetESTALLAMIENTO_HUELGA());
        ps.setString(34, a.GetDECLARA_LICITUD_HUELGA());
        ps.setString(35, a.GetDECLARA_EXISTEN_HUELGA());

        ps.setString(36, a.GetESTATUS_EXPEDIENTE());
        ps.setString(37, a.GetFECHA_ACTO_PROCESAL());
        ps.setString(38, a.GetFASE_SOLI_EXPEDIENTE());
        ps.setString(39, a.GetFORMA_SOLUCION_EMPLAZ());
        ps.setString(40, a.GetESPECIFI_FORMA_EMPLAZ());

        ps.setString(41, a.GetFECHA_RESOLU_EMPLAZ());
        ps.setString(42, a.GetINCREMENTO_SOLICITADO());
        ps.setString(43, a.GetINCREMENTO_OTORGADO());
        ps.setString(44, a.GetFORMA_SOLUCION_HUELGA());
        ps.setString(45, a.GetESPECIFI_FORMA_HUELGA());

        ps.setString(46, a.GetFECHA_RESOLU_HUELGA());
        ps.setString(47, a.GetTIPO_SENTENCIA());
        ps.setString(48, a.GetFECHA_ESTALLAM_HUELGA());
        ps.setString(49, a.GetFECHA_LEVANT_HUELGA());
        ps.setString(50, a.GetDIAS_HUELGA());

        ps.setString(51, a.GetMONTO_ESTIPULADO());
        ps.setString(52, a.GetSALARIOS_CAIDOS());
        ps.setString(53, a.GetCOMENTARIOS());

    }

    private static void insertarBloqueUnoAUno(Connection con, Connection conErr, PreparedStatement ps,
            ArrayList<Huelga> ad, int inicio, int fin, String tablaDestino) throws SQLException {

        for (int i = inicio; i < fin; i++) {
            Huelga a = ad.get(i);

            try {
                setParamsHuelga(ps, a);
                ps.executeUpdate(); // 👈 uno a uno
            } catch (SQLException e) {
                String raw = "idx=" + i
                        + "|CLAVE_ORGANO=" + a.GetCLAVE_ORGANO()
                        + "|EXPEDIENTE_CLAVE=" + a.GetEXPEDIENTE_CLAVE()
                        + "|ID=" + "";
                guardarError(conErr, tablaDestino, a, e, raw);
            }
        }
    }

}
