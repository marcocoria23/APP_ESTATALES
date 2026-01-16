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
import Bean_Procedures.Colectivo;
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
public class ReadCSV_Colectivo {

    public static String impErro = "", RutaT = "", CampoNotConver = "";
    public static int TotalRegistros = 0;
    public static boolean borra_ruta = false;
    ArrayList Array;
    public static String rutaCarpetaArchivos = "";
    Convertir_utf8 conUTF8 = new Convertir_utf8();

    public void Read_Colectivo(Connection con, Connection conErr) throws FileNotFoundException, IOException {
        InsertaTR Insert = new InsertaTR();
        //FileInputStream f = new FileInputStream(Insert.rutaT);  
        try {
            System.out.println("1.....");
            if (Insert.CarpetaArchivos == true) {
                System.out.println("2.....");
                rutaCarpetaArchivos = Insert.rutaT + "CSV_BD_T.3.1_esp_colec.csv";
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
                        IN_COLECTIVO(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("6.....");
                        conUTF8.Convertir_utf8_EBaseDatos(rutaCarpetaArchivos);
                        IN_COLECTIVO(conUTF8.rutaNuevoArchivo, con, conErr);
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
                        IN_COLECTIVO(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("9.....");
                        conUTF8.Convertir_utf8(rutaCarpetaArchivos);
                        IN_COLECTIVO(conUTF8.rutaNuevoArchivo, con, conErr);
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

    public void IN_COLECTIVO(String Ruta, Connection con, Connection conErr) throws Exception {
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
                    if (numeroColumnas == 68) {
                        System.out.println("+hellooou+" + numeroColumnas);
                        cargando cargar = new cargando();
                        ArrayList<Colectivo> ad = new ArrayList<>();
                        for (CSVRecord record : csvParser) {
                            // System.out.println("llenado de csv");
                            TotalRegistros++;
                            Colectivo c = new Colectivo();
                            c.SetNOMBRE_ORGANO_JURIS(record.get(0).toUpperCase());
                            c.SetCLAVE_ORGANO(record.get(1).toUpperCase());
                            c.SetEXPEDIENTE_CLAVE(record.get(2).toUpperCase().replace("\\n", "").trim());
                            c.SetFECHA_APERTURA_EXPEDIENTE(conver.toH2Date(record.get(3).toUpperCase(), "FECHA_APERTURA_EXPEDIENTE"));
                            c.SetTIPO_ASUNTO(conver.CON_V3_TC_TIPO_ASUNTOJL(con, record.get(4).toUpperCase()));
                            c.SetNAT_CONFLICTO(conver.CON_V3_TC_NAT_CONFLICTOJL(con, record.get(5).toUpperCase()));
                            c.SetRAMA_INDUS_INVOLUCRAD(record.get(6).toUpperCase());
                            c.SetSECTOR_RAMA(conver.CON_V3_TC_SECTOR_RAMAJL(con, record.get(7).toUpperCase()));
                            c.SetSUBSECTOR_RAMA(conver.CON_V3_TC_SUBSECTOR_RAMAJL(con, record.get(8).toUpperCase()));
                            c.SetENTIDAD_CLAVE(record.get(9).toUpperCase());
                            c.SetENTIDAD_NOMBRE(record.get(10).toUpperCase());
                            c.SetMUNICIPIO_CLAVE(record.get(11).toUpperCase());
                            if (!record.get(11).toUpperCase().equals("")) {
                                if (record.get(11).toUpperCase().length() == 1) {
                                    c.SetMUNICIPIO_CLAVE(record.get(9).toUpperCase() + "00" + record.get(11).toUpperCase());
                                }
                                if (record.get(11).toUpperCase().length() == 2) {
                                    c.SetMUNICIPIO_CLAVE(record.get(9).toUpperCase() + "0" + record.get(11).toUpperCase());
                                }
                                if (record.get(11).toUpperCase().length() == 5) {
                                } else {
                                    c.SetMUNICIPIO_CLAVE(record.get(11).toUpperCase());
                                }
                            } else {
                                c.SetMUNICIPIO_CLAVE(record.get(11).toUpperCase());
                            }
                            c.SetMUNICIPIO_NOMBRE(record.get(12).toUpperCase());
                            c.SetDECLARACION_PERDIDA_MAY(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(13).toUpperCase()));
                            c.SetSUSPENSION_TMP(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(14).toUpperCase()));
                            c.SetTERMINACION_TRAB(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(15).toUpperCase()));
                            c.SetCONTRATACION_COLECTIVA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(16).toUpperCase()));
                            c.SetOMISIONES_REGLAMENTO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(17).toUpperCase()));
                            c.SetREDUCCION_PERSONAL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(18).toUpperCase()));
                            c.SetVIOLA_DERECHOS(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(19).toUpperCase()));
                            c.SetELECCION_SINDICALES(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(20).toUpperCase()));
                            c.SetSANCION_SINDICALES(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(21).toUpperCase()));
                            c.SetOTRO_CONFLICTO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(22).toUpperCase()));
                            c.SetOTRO_ESP_CONFLICTO(record.get(23).toUpperCase());
                            c.SetNO_IMPUTABLE_ST(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(24).toUpperCase()));
                            c.SetINCAPACIDAD_FISICA_ST(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(25).toUpperCase()));
                            c.SetFALTA_MATERIA_PRIM_ST(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(26).toUpperCase()));
                            c.SetFALTA_MINISTRACION_ST(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(27).toUpperCase()));
                            c.SetFUERZA_MAYOR_TC(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(28).toUpperCase()));
                            c.SetINCAPACIDAD_FISICA_TC(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(29).toUpperCase()));
                            c.SetQUIEBRA_LEGAL_TC(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(30).toUpperCase()));
                            c.SetAGOTAMIENTO_MATERIA_TC(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(31).toUpperCase()));
                            c.SetLIBERTAD_ASOCIACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(32).toUpperCase()));
                            c.SetLIBERTAD_SINDICAL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(33).toUpperCase()));
                            c.SetDERECHO_COLECTIVA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(34).toUpperCase()));
                            c.SetOTRO_COLECTIVA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(35).toUpperCase()));
                            c.SetOTRO_ESP_COLECTIVA(record.get(36).toUpperCase());
                            c.SetINCOMPETENCIA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(37).toUpperCase()));
                            c.SetTIPO_INCOMPETENCIA(conver.CON_V3_TC_TIPO_INCOMPETENCIAJL(con, record.get(38).toUpperCase()));
                            c.SetOTRO_ESP_INCOMP(record.get(39).toUpperCase());
                            c.SetFECHA_PRES_DEMANDA(conver.toH2Date(record.get(40).toUpperCase(), "FECHA_PRES_DEMANDA"));
                            c.SetCONSTANCIA_CONS_EXPEDIDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(41).toUpperCase()));
                            c.SetCONSTANCIA_CLAVE(record.get(42).toUpperCase());
                            c.SetASUN_EXCEP_CONCILIACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(43).toUpperCase()));
                            c.SetPREVE_DEMANDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(44).toUpperCase()));
                            c.SetDESAHOGO_PREV_DEMANDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(45).toUpperCase()));
                            c.SetESTATUS_DEMANDA(conver.CON_V3_TC_ESTATUS_DEMANDAJL(con, record.get(46).toUpperCase()));
                            c.SetFECHA_ADMI_DEMANDA(conver.toH2Date(record.get(47).toUpperCase(), "FECHA_ADMI_DEMANDA"));
                            c.SetCANTIDAD_ACTORES(record.get(48).toUpperCase());
                            c.SetCANTIDAD_DEMANDADOS(record.get(49).toUpperCase());
                            c.SetAUTO_DEPURACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(50).toUpperCase()));
                            c.SetFECHA_DEPURACION(conver.toH2Date(record.get(51).toUpperCase(), "FECHA_DEPURACION"));
                            c.SetAUDIENCIA_JUICIO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(52).toUpperCase()));
                            c.SetFECHA_AUDIENCIA_JUICIO(conver.toH2Date(record.get(53).toUpperCase(), "FECHA_AUDIENCIA_JUICIO"));
                            c.SetESTATUS_EXPEDIENTE(conver.CON_V3_TC_ESTATUS_EXPEDIENTEJL(con, record.get(54).toUpperCase()));
                            c.SetFECHA_ACTO_PROCESAL(conver.toH2Date(record.get(55).toUpperCase(), "FECHA_ACTO_PROCESAL"));
                            c.SetFASE_SOLI_EXPEDIENTE(conver.CON_V3_TC_FASE_EXPEDIENTEJL(con, record.get(56).toUpperCase()));
                            c.SetFORMA_SOLUCION_AD(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(57).toUpperCase()));
                            c.SetOTRO_ESP_SOLUCION_AD(record.get(58).toUpperCase());
                            c.SetFECHA_DICTO_RESOLUCION_AD(conver.toH2Date(record.get(59).toUpperCase(), "FECHA_DICTO_RESOLUCION_AD"));
                            c.SetTIPO_SENTENCIA_AD(conver.CON_V3_TC_TIPO_SENTENCIAJL(con, record.get(60).toUpperCase()));
                            c.SetMONTO_SOLUCION_AD(record.get(61).toUpperCase());
                            c.SetFORMA_SOLUCION_AJ(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(62).toUpperCase()));
                            c.SetOTRO_ESP_SOLUCION_AJ(record.get(63).toUpperCase());
                            c.SetFECHA_RESOLUCION_AJ(conver.toH2Date(record.get(64).toUpperCase(), "FECHA_RESOLUCION_AJ"));
                            c.SetTIPO_SENTENCIA_AJ(conver.CON_V3_TC_TIPO_SENTENCIAJL(con, record.get(65).toUpperCase()));
                            c.SetMONTO_SOLUCION_AJ(record.get(66).toUpperCase());
                            c.SetCOMENTARIOS(record.get(67).toUpperCase());
                            ad.add(c);
                        }
                        System.out.println("entro 1");
                        if (TotalRegistros > 0) {
                            if (Inserta == true) {
                                System.out.println("entro 2");
                                cargar.setVisible(true);
                                // OJO: AJUSTA los nombres reales de columnas en tu tabla H2
                                final String sql
                                        = "INSERT INTO V3_TR_COLECTIVOJL ("
                                        + "NOMBRE_ORGANO_JURIS, CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, TIPO_ASUNTO,\n"
                                        + "NAT_CONFLICTO, RAMA_INDUS_INVOLUCRAD, SECTOR_RAMA, SUBSECTOR_RAMA, ENTIDAD_CLAVE,\n"
                                        + "ENTIDAD_NOMBRE, MUNICIPIO_CLAVE, MUNICIPIO_NOMBRE, DECLARACION_PERDIDA_MAY, SUSPENSION_TMP,\n"
                                        + "TERMINACION_TRAB, CONTRATACION_COLECTIVA, OMISIONES_REGLAMENTO, REDUCCION_PERSONAL, VIOLA_DERECHOS,\n"
                                        + "ELECCION_SINDICALES, SANCION_SINDICALES, OTRO_CONFLICTO, OTRO_ESP_CONFLICTO, NO_IMPUTABLE_ST,\n"
                                        + "INCAPACIDAD_FISICA_ST, FALTA_MATERIA_PRIM_ST, FALTA_MINISTRACION_ST, FUERZA_MAYOR_TC, INCAPACIDAD_FISICA_TC,\n"
                                        + "QUIEBRA_LEGAL_TC, AGOTAMIENTO_MATERIA_TC, LIBERTAD_ASOCIACION, LIBERTAD_SINDICAL, DERECHO_COLECTIVA,\n"
                                        + "OTRO_COLECTIVA, OTRO_ESP_COLECTIVA, INCOMPETENCIA, TIPO_INCOMPETENCIA, OTRO_ESP_INCOMP,\n"
                                        + "FECHA_PRES_DEMANDA, CONSTANCIA_CONS_EXPEDIDA, CONSTANCIA_CLAVE, ASUN_EXCEP_CONCILIACION, PREVE_DEMANDA,\n"
                                        + "DESAHOGO_PREV_DEMANDA, ESTATUS_DEMANDA, FECHA_ADMI_DEMANDA, CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS,\n"
                                        + "AUTO_DEPURACION, FECHA_DEPURACION, AUDIENCIA_JUICIO, FECHA_AUDIENCIA_JUICIO, ESTATUS_EXPEDIENTE,\n"
                                        + "FECHA_ACTO_PROCESAL, FASE_SOLI_EXPEDIENTE, FORMA_SOLUCION_AD, OTRO_ESP_SOLUCION_AD, FECHA_DICTO_RESOLUCION_AD,\n"
                                        + "TIPO_SENTENCIA_AD, MONTO_SOLUCION_AD, FORMA_SOLUCION_AJ, OTRO_ESP_SOLUCION_AJ, FECHA_RESOLUCION_AJ,\n"
                                        + "TIPO_SENTENCIA_AJ, MONTO_SOLUCION_AJ, COMENTARIOS"
                                        + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
                                        Colectivo a = ad.get(i);
                                        // ✅ usa tu helper para setear params (mismo orden de tu INSERT)
                                        setParamsColectivo(ps, a);
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
                                                insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, blockEnd, "V3_TR_COLECTIVOJL");

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
                                            insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, ad.size(), "V3_TR_COLECTIVOJL");
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
                            JOptionPane.showMessageDialog(null, "Archivo .CSV sin Registros-V3_TMP_COLECTIVOJL");
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

    private static void guardarError(Connection conErr, String tablaDestino, Colectivo a,
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

    private static void setParamsColectivo(PreparedStatement ps, Colectivo a) throws SQLException {
        ps.setString(1, a.GetNOMBRE_ORGANO_JURIS());
        ps.setString(2, a.GetCLAVE_ORGANO());
        ps.setString(3, a.GetEXPEDIENTE_CLAVE());
        ps.setString(4, a.GetFECHA_APERTURA_EXPEDIENTE());
        ps.setString(5, a.GetTIPO_ASUNTO());

        ps.setString(6, a.GetNAT_CONFLICTO());
        ps.setString(7, a.GetRAMA_INDUS_INVOLUCRAD());
        ps.setString(8, a.GetSECTOR_RAMA());
        ps.setString(9, a.GetSUBSECTOR_RAMA());
        ps.setString(10, a.GetENTIDAD_CLAVE());

        ps.setString(11, a.GetENTIDAD_NOMBRE());
        ps.setString(12, a.GetMUNICIPIO_CLAVE());
        ps.setString(13, a.GetMUNICIPIO_NOMBRE());
        ps.setString(14, a.GetDECLARACION_PERDIDA_MAY());
        ps.setString(15, a.GetSUSPENSION_TMP());

        ps.setString(16, a.GetTERMINACION_TRAB());
        ps.setString(17, a.GetCONTRATACION_COLECTIVA());
        ps.setString(18, a.GetOMISIONES_REGLAMENTO());
        ps.setString(19, a.GetREDUCCION_PERSONAL());
        ps.setString(20, a.GetVIOLA_DERECHOS());

        ps.setString(21, a.GetELECCION_SINDICALES());
        ps.setString(22, a.GetSANCION_SINDICALES());
        ps.setString(23, a.GetOTRO_CONFLICTO());
        ps.setString(24, a.GetOTRO_ESP_CONFLICTO());
        ps.setString(25, a.GetNO_IMPUTABLE_ST());

        ps.setString(26, a.GetINCAPACIDAD_FISICA_ST());
        ps.setString(27, a.GetFALTA_MATERIA_PRIM_ST());
        ps.setString(28, a.GetFALTA_MINISTRACION_ST());
        ps.setString(29, a.GetFUERZA_MAYOR_TC());
        ps.setString(30, a.GetINCAPACIDAD_FISICA_TC());

        ps.setString(31, a.GetQUIEBRA_LEGAL_TC());
        ps.setString(32, a.GetAGOTAMIENTO_MATERIA_TC());
        ps.setString(33, a.GetLIBERTAD_ASOCIACION());
        ps.setString(34, a.GetLIBERTAD_SINDICAL());
        ps.setString(35, a.GetDERECHO_COLECTIVA());

        ps.setString(36, a.GetOTRO_COLECTIVA());
        ps.setString(37, a.GetOTRO_ESP_COLECTIVA());
        ps.setString(38, a.GetINCOMPETENCIA());
        ps.setString(39, a.GetTIPO_INCOMPETENCIA());
        ps.setString(40, a.GetOTRO_ESP_INCOMP());

        ps.setString(41, a.GetFECHA_PRES_DEMANDA());
        ps.setString(42, a.GetCONSTANCIA_CONS_EXPEDIDA());
        ps.setString(43, a.GetCONSTANCIA_CLAVE());
        ps.setString(44, a.GetASUN_EXCEP_CONCILIACION());
        ps.setString(45, a.GetPREVE_DEMANDA());

        ps.setString(46, a.GetDESAHOGO_PREV_DEMANDA());
        ps.setString(47, a.GetESTATUS_DEMANDA());
        ps.setString(48, a.GetFECHA_ADMI_DEMANDA());
        ps.setString(49, a.GetCANTIDAD_ACTORES());
        ps.setString(50, a.GetCANTIDAD_DEMANDADOS());

        ps.setString(51, a.GetAUTO_DEPURACION());
        ps.setString(52, a.GetFECHA_DEPURACION());
        ps.setString(53, a.GetAUDIENCIA_JUICIO());
        ps.setString(54, a.GetFECHA_AUDIENCIA_JUICIO());
        ps.setString(55, a.GetESTATUS_EXPEDIENTE());

        ps.setString(56, a.GetFECHA_ACTO_PROCESAL());
        ps.setString(57, a.GetFASE_SOLI_EXPEDIENTE());
        ps.setString(58, a.GetFORMA_SOLUCION_AD());
        ps.setString(59, a.GetOTRO_ESP_SOLUCION_AD());
        ps.setString(60, a.GetFECHA_DICTO_RESOLUCION_AD());

        ps.setString(61, a.GetTIPO_SENTENCIA_AD());
        ps.setString(62, a.GetMONTO_SOLUCION_AD());
        ps.setString(63, a.GetFORMA_SOLUCION_AJ());
        ps.setString(64, a.GetOTRO_ESP_SOLUCION_AJ());
        ps.setString(65, a.GetFECHA_RESOLUCION_AJ());

        ps.setString(66, a.GetTIPO_SENTENCIA_AJ());
        ps.setString(67, a.GetMONTO_SOLUCION_AJ());
        ps.setString(68, a.GetCOMENTARIOS());

    }

    private static void insertarBloqueUnoAUno(Connection con, Connection conErr, PreparedStatement ps,
            ArrayList<Colectivo> ad, int inicio, int fin, String tablaDestino) throws SQLException {

        for (int i = inicio; i < fin; i++) {
            Colectivo a = ad.get(i);

            try {
                setParamsColectivo(ps, a);
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
