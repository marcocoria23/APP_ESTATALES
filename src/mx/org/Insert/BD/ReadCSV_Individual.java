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
import Bean_Procedures.Individual;
import ConverCat.Convers;
import Pantallas_laborales.cargando;
import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;

/**
 *
 * @author ANDREA.HERNANDEZL
 */
public class ReadCSV_Individual {

    public static String impErro = "", RutaT = "", CampoNotConver = "";
    public static int TotalRegistros = 0;
    public static boolean borra_ruta = false;
    ArrayList Array;
    public static String rutaCarpetaArchivos = "";
    Convertir_utf8 conUTF8 = new Convertir_utf8();

    public void Read_Individual(Connection con, Connection conErr) throws FileNotFoundException, IOException {
        InsertaTR Insert = new InsertaTR();
        //FileInputStream f = new FileInputStream(Insert.rutaT);  
        try {
            System.out.println("1.....");
            if (Insert.CarpetaArchivos == true) {
                System.out.println("2.....");
                rutaCarpetaArchivos = Insert.rutaT + "CSV_BD_T.2.1_esp_indiv.csv";
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
                        IN_INDIVIDUAL(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("6.....");
                        conUTF8.Convertir_utf8_EBaseDatos(rutaCarpetaArchivos);
                        IN_INDIVIDUAL(conUTF8.rutaNuevoArchivo, con, conErr);
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
                        IN_INDIVIDUAL(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("9.....");
                        conUTF8.Convertir_utf8(rutaCarpetaArchivos);
                        IN_INDIVIDUAL(conUTF8.rutaNuevoArchivo, con, conErr);
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

    public void IN_INDIVIDUAL(String Ruta, Connection con, Connection conErr) throws Exception {
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
                    if (numeroColumnas == 78) {
                        System.out.println("+hellooou+" + numeroColumnas);
                        cargando cargar = new cargando();
                        ArrayList<Individual> ad = new ArrayList<>();
                        for (CSVRecord record : csvParser) {
                            // System.out.println("llenado de csv");
                            TotalRegistros++;
                            Individual c = new Individual();
                            c.SetNOMBRE_ORGANO_JURIS(record.get(0).toUpperCase());
                            c.SetCLAVE_ORGANO(record.get(1).toUpperCase());
                            c.SetEXPEDIENTE_CLAVE(record.get(2).toUpperCase().replace("\\n", "").trim());
                            c.SetFECHA_APERTURA_EXPEDIENTE(conver.toH2Date(record.get(3).toUpperCase(), "FECHA_APERTURA_EXPEDIENT"));
                            c.SetTIPO_ASUNTO(conver.CON_V3_TC_TIPO_ASUNTOJL(con, record.get(4).toUpperCase()));
                            c.SetNAT_CONFLICTO(conver.CON_V3_TC_NAT_CONFLICTOJL(con, record.get(5).toUpperCase()));
                            c.SetCONTRATO_ESCRITO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(6).toUpperCase()));
                            c.SetTIPO_CONTRATO(conver.CON_V3_TC_TIPO_CONTRATOJL(con, record.get(7).toUpperCase()));
                            c.SetRAMA_INDUS_INVOLUCRADA(record.get(8).toUpperCase());
                            c.SetSECTOR_RAMA(conver.CON_V3_TC_SECTOR_RAMAJL(con, record.get(9).toUpperCase()));
                            c.SetSUBSECTOR_RAMA(conver.CON_V3_TC_SUBSECTOR_RAMAJL(con, record.get(10).toUpperCase()));
                            c.SetENTIDAD_CLAVE(record.get(11).toUpperCase());
                            c.SetENTIDAD_NOMBRE(record.get(12).toUpperCase());
                            c.SetMUNICIPIO_CLAVE(record.get(13).toUpperCase());
                            if (!record.get(13).toUpperCase().equals("")) {
                                if (record.get(13).toUpperCase().length() == 1) {
                                    c.SetMUNICIPIO_CLAVE(record.get(11).toUpperCase() + "00" + record.get(13).toUpperCase());
                                }
                                if (record.get(16).toUpperCase().length() == 2) {
                                    c.SetMUNICIPIO_CLAVE(record.get(11).toUpperCase() + "0" + record.get(13).toUpperCase());
                                }
                                if (record.get(16).toUpperCase().length() == 5) {
                                } else {
                                    c.SetMUNICIPIO_CLAVE(record.get(13).toUpperCase());
                                }
                            } else {
                                c.SetMUNICIPIO_CLAVE(record.get(13).toUpperCase());
                            }
                            c.SetMUNICIPIO_NOMBRE(record.get(14).toUpperCase());
                            c.SetSUBCONTRATACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(15).toUpperCase()));
                            c.SetINDOLE_TRABAJO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(16).toUpperCase()));
                            c.SetPRESTACION_FP(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(17).toUpperCase()));
                            c.SetARRENDAM_TRAB(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(18).toUpperCase()));
                            c.SetCAPACITACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(19).toUpperCase()));
                            c.SetANTIGUEDAD(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(20).toUpperCase()));
                            c.SetPRIMA_ANTIGUEDAD(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(21).toUpperCase()));
                            c.SetCONVENIO_TRAB(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(22).toUpperCase()));
                            c.SetDESIGNACION_TRAB_FALLE(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(23).toUpperCase()));
                            c.SetDESIGNACION_TRAB_ACT_DELIC(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(24).toUpperCase()));
                            c.SetTERMINACION_LAB(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(25).toUpperCase()));
                            c.SetRECUPERACION_CARGA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(26).toUpperCase()));
                            c.SetGASTOS_TRASLADOS(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(27).toUpperCase()));
                            c.SetINDEMNIZACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(28).toUpperCase()));
                            c.SetPAGO_INDEMNIZACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(29).toUpperCase()));
                            c.SetDESACUERDO_MEDICOS(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(30).toUpperCase()));
                            c.SetCOBRO_PRESTACIONES(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(31).toUpperCase()));
                            c.SetCONF_SEGURO_SOCIAL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(32).toUpperCase()));
                            c.SetOTRO_CONF(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(33).toUpperCase()));
                            c.SetOTRO_ESP_CONF(record.get(34).toUpperCase());
                            c.SetINCOMPETENCIA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(35).toUpperCase()));
                            c.SetTIPO_INCOMPETENCIA(conver.CON_V3_TC_TIPO_INCOMPETENCIAJL(con, record.get(36).toUpperCase()));
                            c.SetOTRO_ESP_INCOMP(record.get(37).toUpperCase());
                            c.SetFECHA_PRES_DEMANDA(conver.toH2Date(record.get(38).toUpperCase(), "FECHA_PRES_DEMANDA"));
                            c.SetCONSTANCIA_CONS_EXPEDIDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(39).toUpperCase()));
                            c.SetCONSTANCIA_CLAVE(record.get(40).toUpperCase());
                            c.SetASUN_EXCEP_CONCILIACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(41).toUpperCase()));
                            c.SetPREVE_DEMANDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(42).toUpperCase()));
                            c.SetDESAHOGO_PREV_DEMANDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(43).toUpperCase()));
                            c.SetESTATUS_DEMANDA(conver.CON_V3_TC_ESTATUS_DEMANDAJL(con, record.get(44).toUpperCase()));
                            c.SetCAU_IMPI_ADMI_DEMANDA(conver.CON_V3_TC_CAU_IMPI_ADMI_DEMJL(con, record.get(45).toUpperCase()));
                            c.SetFECHA_ADMI_DEMANDA(conver.toH2Date(record.get(46).toUpperCase(), "FECHA_ADMI_DEMANDA"));
                            c.SetCANTIDAD_ACTORES(record.get(47).toUpperCase());
                            c.SetCANTIDAD_DEMANDADOS(record.get(48).toUpperCase());
                            c.SetTRAMITACION_DEPURACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(49).toUpperCase()));
                            c.SetFECHA_DEPURACION(conver.toH2Date(record.get(50).toUpperCase(), "FECHA_DEPURACION"));
                            c.SetAUDIENCIA_PRELIM(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(51).toUpperCase()));
                            c.SetFECHA_AUDIENCIA_PRELIM(conver.toH2Date(record.get(52).toUpperCase(), "FECHA_AUDIENCIA_PRELIM"));
                            c.SetAUDIENCIA_JUICIO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(53).toUpperCase()));
                            c.SetFECHA_AUDIENCIA_JUICIO(conver.toH2Date(record.get(54).toUpperCase(), "FECHA_AUDIENCIA_JUICIO"));
                            c.SetESTATUS_EXPEDIENTE(conver.CON_V3_TC_ESTATUS_EXPEDIENTEJL(con, record.get(55).toUpperCase()));
                            c.SetFECHA_ACTO_PROCESAL(conver.toH2Date(record.get(56).toUpperCase(), "FECHA_ACTO_PROCESAL"));
                            c.SetFASE_SOLI_EXPEDIENTE(conver.CON_V3_TC_FASE_EXPEDIENTEJL(con, record.get(57).toUpperCase()));
                            c.SetFORMA_SOLUCION_AD(record.get(58).toUpperCase());
                            c.SetOTRO_ESP_SOLUCION_AD(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(59).toUpperCase()));
                            c.SetFECHA_DICTO_RESOLUCION_AD(conver.toH2Date(record.get(60).toUpperCase(), "FECHA_DICTO_RESOLUCION_AD"));
                            c.SetTIPO_SENTENCIA_AD(conver.CON_V3_TC_TIPO_SENTENCIAJL(con, record.get(61).toUpperCase()));
                            c.SetMONTO_SOLUCION_AD(record.get(62).toUpperCase());
                            c.SetFORMA_SOLUCION_TA(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(63).toUpperCase()));
                            c.SetOTRO_ESP_SOLUCION_TA(record.get(64).toUpperCase());
                            c.SetFECHA_RESOLUCION_TA(conver.toH2Date(record.get(65).toUpperCase(), "FECHA_RESOLUCION_TA"));
                            c.SetTIPO_SENTENCIA_TA(conver.CON_V3_TC_TIPO_SENTENCIAJL(con, record.get(66).toUpperCase()));
                            c.SetMONTO_SOLUCIÓN_TA(record.get(67).toUpperCase());
                            c.SetFORMA_SOLUCION_AP(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(68).toUpperCase()));
                            c.SetOTRO_ESP_SOLUCION_AP(record.get(69).toUpperCase());
                            c.SetFECHA_DICTO_RESOLUCION_AP(conver.toH2Date(record.get(70).toUpperCase(), "FECHA_DICTO_RESOLUCION_AP"));
                            c.SetMONTO_SOLUCION_AP(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(71).toUpperCase()));
                            c.SetFORMA_SOLUCION_AJ(record.get(72).toUpperCase());
                            c.SetOTRO_ESP_SOLUCION_AJ(record.get(73).toUpperCase());
                            c.SetFECHA_DICTO_RESOLUCION_AJ(conver.toH2Date(record.get(74).toUpperCase(), "FECHA_DICTO_RESOLUCION_AJ"));
                            c.SetTIPO_SENTENCIA_AJ(conver.CON_V3_TC_TIPO_SENTENCIAJL(con, record.get(75).toUpperCase()));
                            c.SetMONTO_SOLUCIÓN_AJ(record.get(76).toUpperCase());
                            c.SetCOMENTARIOS(record.get(77).toUpperCase());
                            ad.add(c);
                        }
                        System.out.println("entro 1");
                        if (TotalRegistros > 0) {
                            if (Inserta == true) {
                                System.out.println("entro 2");
                                cargar.setVisible(true);
                                // OJO: AJUSTA los nombres reales de columnas en tu tabla H2
                                final String sql
                                        = "INSERT INTO V3_TR_INDIVIDUALJL ("
                                        + "NOMBRE_ORGANO_JURIS, CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, TIPO_ASUNTO,\n"
                                        + "NAT_CONFLICTO, CONTRATO_ESCRITO, TIPO_CONTRATO, RAMA_INDUS_INVOLUCRADA, SECTOR_RAMA,\n"
                                        + "SUBSECTOR_RAMA, ENTIDAD_CLAVE, ENTIDAD_NOMBRE, MUNICIPIO_CLAVE, MUNICIPIO_NOMBRE,\n"
                                        + "SUBCONTRATACION, INDOLE_TRABAJO, PRESTACION_FP, ARRENDAM_TRAB, CAPACITACION,\n"
                                        + "ANTIGUEDAD, PRIMA_ANTIGUEDAD, CONVENIO_TRAB, DESIGNACION_TRAB_FALLE, DESIGNACION_TRAB_ACT_DELIC,\n"
                                        + "TERMINACION_LAB, RECUPERACION_CARGA, GASTOS_TRASLADOS, INDEMNIZACION, PAGO_INDEMNIZACION,\n"
                                        + "DESACUERDO_MEDICOS, COBRO_PRESTACIONES, CONF_SEGURO_SOCIAL, OTRO_CONF, OTRO_ESP_CONF,\n"
                                        + "INCOMPETENCIA, TIPO_INCOMPETENCIA, OTRO_ESP_INCOMP, FECHA_PRES_DEMANDA, CONSTANCIA_CONS_EXPEDIDA,\n"
                                        + "CONSTANCIA_CLAVE, ASUN_EXCEP_CONCILIACION, PREVE_DEMANDA, DESAHOGO_PREV_DEMANDA, ESTATUS_DEMANDA,\n"
                                        + "CAU_IMPI_ADMI_DEMANDA, FECHA_ADMI_DEMANDA, CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS, TRAMITACION_DEPURACION,\n"
                                        + "FECHA_DEPURACION, AUDIENCIA_PRELIM, FECHA_AUDIENCIA_PRELIM, AUDIENCIA_JUICIO, FECHA_AUDIENCIA_JUICIO,\n"
                                        + "ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL, FASE_SOLI_EXPEDIENTE, FORMA_SOLUCION_AD, OTRO_ESP_SOLUCION_AD,\n"
                                        + "FECHA_DICTO_RESOLUCION_AD, TIPO_SENTENCIA_AD, MONTO_SOLUCION_AD, FORMA_SOLUCION_TA, OTRO_ESP_SOLUCION_TA,\n"
                                        + "FECHA_RESOLUCION_TA, TIPO_SENTENCIA_TA, MONTO_SOLUCION_TA, FORMA_SOLUCION_AP, OTRO_ESP_SOLUCION_AP,\n"
                                        + "FECHA_DICTO_RESOLUCION_AP, MONTO_SOLUCION_AP, FORMA_SOLUCION_AJ, OTRO_ESP_SOLUCION_AJ,\n"
                                        + "FECHA_DICTO_RESOLUCION_AJ, TIPO_SENTENCIA_AJ, MONTO_SOLUCION_AJ, COMENTARIOS"
                                        + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
                                        Individual a = ad.get(i);
                                        // ✅ usa tu helper para setear params (mismo orden de tu INSERT)
                                        setParamsIndividual(ps, a);
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
                                                insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, blockEnd, "V3_TR_INDIVIDUALJL");

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
                                            insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, ad.size(), "V3_TR_INDIVIDUALJL");
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
                            JOptionPane.showMessageDialog(null, "Archivo .CSV sin Registros-V3_TR_INDIVIDUALJL");
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

    private static void guardarError(Connection conErr, String tablaDestino, Individual a,
            SQLException e, String raw) {
        try ( PreparedStatement pe = conErr.prepareStatement(SQL_INSERT_ERROR)) {
            pe.setString(1, tablaDestino);
            pe.setString(2, a.GetCLAVE_ORGANO());
            pe.setString(3, a.GetEXPEDIENTE_CLAVE());
            pe.setString(4, "");
            pe.setString(5, e.getSQLState());
            pe.setInt(6, e.getErrorCode());
            String msg = e.getMessage().replace("Violación de indice de Unicidad ó Clave primaria", "Registro Duplicado");
            pe.setString(7, msg != null && msg.length() > 500 ? msg.substring(0, 250) : msg);
            pe.setString(8, raw);
            pe.executeUpdate();
        } catch (SQLException ex) {
            // Si hasta la tabla de errores falla, al menos lo imprimimos
            System.err.println("❌ No se pudo guardar en ERRORES_INSERT: " + ex.getMessage());
        }
    }

    private static void setParamsIndividual(PreparedStatement ps, Individual a) throws SQLException {
        ps.setString(1, a.GetNOMBRE_ORGANO_JURIS());
        ps.setString(2, a.GetCLAVE_ORGANO());
        ps.setString(3, a.GetEXPEDIENTE_CLAVE());
        ps.setString(4, a.GetFECHA_APERTURA_EXPEDIENTE());
        ps.setString(5, a.GetTIPO_ASUNTO());

        ps.setString(6, a.GetNAT_CONFLICTO());
        ps.setString(7, a.GetCONTRATO_ESCRITO());
        ps.setString(8, a.GetTIPO_CONTRATO());
        ps.setString(9, a.GetRAMA_INDUS_INVOLUCRADA());
        ps.setString(10, a.GetSECTOR_RAMA());

        ps.setString(11, a.GetSUBSECTOR_RAMA());
        ps.setString(12, a.GetENTIDAD_CLAVE());
        ps.setString(13, a.GetENTIDAD_NOMBRE());
        ps.setString(14, a.GetMUNICIPIO_CLAVE());
        ps.setString(15, a.GetMUNICIPIO_NOMBRE());

        ps.setString(16, a.GetSUBCONTRATACION());
        ps.setString(17, a.GetINDOLE_TRABAJO());
        ps.setString(18, a.GetPRESTACION_FP());
        ps.setString(19, a.GetARRENDAM_TRAB());
        ps.setString(20, a.GetCAPACITACION());

        ps.setString(21, a.GetANTIGUEDAD());
        ps.setString(22, a.GetPRIMA_ANTIGUEDAD());
        ps.setString(23, a.GetCONVENIO_TRAB());
        ps.setString(24, a.GetDESIGNACION_TRAB_FALLE());
        ps.setString(25, a.GetDESIGNACION_TRAB_ACT_DELIC());

        ps.setString(26, a.GetTERMINACION_LAB());
        ps.setString(27, a.GetRECUPERACION_CARGA());
        ps.setString(28, a.GetGASTOS_TRASLADOS());
        ps.setString(29, a.GetINDEMNIZACION());
        ps.setString(30, a.GetPAGO_INDEMNIZACION());

        ps.setString(31, a.GetDESACUERDO_MEDICOS());
        ps.setString(32, a.GetCOBRO_PRESTACIONES());
        ps.setString(33, a.GetCONF_SEGURO_SOCIAL());
        ps.setString(34, a.GetOTRO_CONF());
        ps.setString(35, a.GetOTRO_ESP_CONF());

        ps.setString(36, a.GetINCOMPETENCIA());
        ps.setString(37, a.GetTIPO_INCOMPETENCIA());
        ps.setString(38, a.GetOTRO_ESP_INCOMP());
        ps.setString(39, a.GetFECHA_PRES_DEMANDA());
        ps.setString(40, a.GetCONSTANCIA_CONS_EXPEDIDA());

        ps.setString(41, a.GetCONSTANCIA_CLAVE());
        ps.setString(42, a.GetASUN_EXCEP_CONCILIACION());
        ps.setString(43, a.GetPREVE_DEMANDA());
        ps.setString(44, a.GetDESAHOGO_PREV_DEMANDA());
        ps.setString(45, a.GetESTATUS_DEMANDA());

        ps.setString(46, a.GetCAU_IMPI_ADMI_DEMANDA());
        ps.setString(47, a.GetFECHA_ADMI_DEMANDA());
        ps.setString(48, a.GetCANTIDAD_ACTORES());
        ps.setString(49, a.GetCANTIDAD_DEMANDADOS());
        ps.setString(50, a.GetTRAMITACION_DEPURACION());

        ps.setString(51, a.GetFECHA_DEPURACION());
        ps.setString(52, a.GetAUDIENCIA_PRELIM());
        ps.setString(53, a.GetFECHA_AUDIENCIA_PRELIM());
        ps.setString(54, a.GetAUDIENCIA_JUICIO());
        ps.setString(55, a.GetFECHA_AUDIENCIA_JUICIO());

        ps.setString(56, a.GetESTATUS_EXPEDIENTE());
        ps.setString(57, a.GetFECHA_ACTO_PROCESAL());
        ps.setString(58, a.GetFASE_SOLI_EXPEDIENTE());
        ps.setString(59, a.GetFORMA_SOLUCION_AD());
        ps.setString(60, a.GetOTRO_ESP_SOLUCION_AD());

        ps.setString(61, a.GetFECHA_DICTO_RESOLUCION_AD());
        ps.setString(62, a.GetTIPO_SENTENCIA_AD());
        ps.setString(63, a.GetMONTO_SOLUCION_AD());
        ps.setString(64, a.GetFORMA_SOLUCION_TA());
        ps.setString(65, a.GetOTRO_ESP_SOLUCION_TA());

        ps.setString(66, a.GetFECHA_RESOLUCION_TA());
        ps.setString(67, a.GetTIPO_SENTENCIA_TA());
        ps.setString(68, a.GetMONTO_SOLUCIÓN_TA());
        ps.setString(69, a.GetFORMA_SOLUCION_AP());
        ps.setString(70, a.GetOTRO_ESP_SOLUCION_AP());

        ps.setString(71, a.GetFECHA_DICTO_RESOLUCION_AP());
        ps.setString(72, a.GetMONTO_SOLUCION_AP());
        ps.setString(73, a.GetFORMA_SOLUCION_AJ());
        ps.setString(74, a.GetOTRO_ESP_SOLUCION_AJ());
        ps.setString(75, a.GetFECHA_DICTO_RESOLUCION_AJ());

        ps.setString(76, a.GetTIPO_SENTENCIA_AJ());
        ps.setString(77, a.GetMONTO_SOLUCIÓN_AJ());
        ps.setString(78, a.GetCOMENTARIOS());

    }

    private static void insertarBloqueUnoAUno(Connection con, Connection conErr, PreparedStatement ps,
            ArrayList<Individual> ad, int inicio, int fin, String tablaDestino) throws SQLException {

        for (int i = inicio; i < fin; i++) {
            Individual a = ad.get(i);

            try {
                setParamsIndividual(ps, a);
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
