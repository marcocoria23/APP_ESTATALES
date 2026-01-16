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
import Bean_Procedures.Ordinario;
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
public class ReadCSV_Ordinario {

    public static String impErro = "", RutaT = "", CampoNotConver = "";
    public static int TotalRegistros = 0;
    public static boolean borra_ruta = false;
    ArrayList Array;
    public static String rutaCarpetaArchivos = "";
    Convertir_utf8 conUTF8 = new Convertir_utf8();

    public void Read_Ordinario(Connection con, Connection conErr) throws FileNotFoundException, IOException {
        InsertaTR Insert = new InsertaTR();
        //FileInputStream f = new FileInputStream(Insert.rutaT);  
        try {
            System.out.println("1.....");
            if (Insert.CarpetaArchivos == true) {
                System.out.println("2.....");
                rutaCarpetaArchivos = Insert.rutaT + "CSV_BD_T.1.1_ordinario.csv";
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
                        IN_ORDINARIO(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("6.....");
                        conUTF8.Convertir_utf8_EBaseDatos(rutaCarpetaArchivos);
                        IN_ORDINARIO(conUTF8.rutaNuevoArchivo, con, conErr);
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
                        IN_ORDINARIO(rutaCarpetaArchivos, con, conErr);
                    } else {
                        System.out.println("9.....");
                        conUTF8.Convertir_utf8(rutaCarpetaArchivos);
                        IN_ORDINARIO(conUTF8.rutaNuevoArchivo, con, conErr);
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

    public void IN_ORDINARIO(String Ruta, Connection con, Connection conErr) throws Exception {
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
                    if (numeroColumnas == 94) {
                        System.out.println("+hellooou+" + numeroColumnas);
                        cargando cargar = new cargando();
                        ArrayList<Ordinario> ad = new ArrayList<>();
                        for (CSVRecord record : csvParser) {
                            // System.out.println("llenado de csv");
                            TotalRegistros++;
                            Ordinario c = new Ordinario();
                            c.SetNOMBRE_ORGANO_JURIS(record.get(0).toUpperCase());
                            c.SetCLAVE_ORGANO(record.get(1).toUpperCase());
                            c.SetEXPEDIENTE_CLAVE(record.get(2).toUpperCase());
                            c.SetFECHA_APERTURA_EXPEDIENTE(conver.toH2Date(record.get(3).toUpperCase(),"FECHA_APERTURA_EXPEDIENTE"));
                            c.SetTIPO_ASUNTO(conver.CON_V3_TC_TIPO_ASUNTOJL(con, record.get(4).toUpperCase()));
                            c.SetNAT_CONFLICTO(conver.CON_V3_TC_NAT_CONFLICTOJL(con, record.get(5).toUpperCase()));
                            c.SetCONTRATO_ESCRITO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(6).toUpperCase()));
                            c.SetTIPO_CONTRATO(conver.CON_V3_TC_TIPO_CONTRATOJL(con, record.get(7).toUpperCase()));
                            c.SetRAMA_INDUS_INVOLUCRADA(record.get(8).toUpperCase());
                            c.SetSECTOR_RAMA(conver.CON_V3_TC_SECTOR_RAMAJL(con, record.get(9).toUpperCase().replace("_", " ")));
                            c.SetSUBSECTOR_RAMA(conver.CON_V3_TC_SUBSECTOR_RAMAJL(con, record.get(10).toUpperCase()));
                            c.SetENTIDAD_CLAVE(record.get(11).toUpperCase());
                            c.SetENTIDAD_NOMBRE(record.get(12).toUpperCase());
                             if (!record.get(13).toUpperCase().equals(""))
                            {
                             if (record.get(13).toUpperCase().length()==1)
                             {
                            c.SetMUNICIPIO_CLAVE(record.get(11).toUpperCase()+"00"+record.get(13).toUpperCase());   
                             }
                             if (record.get(13).toUpperCase().length()==2)
                             {
                            c.SetMUNICIPIO_CLAVE(record.get(11).toUpperCase()+"0"+record.get(13).toUpperCase());     
                             }
                             if(record.get(13).toUpperCase().length()==5)
                             {
                                 c.SetMUNICIPIO_CLAVE(record.get(13).toUpperCase()); 
                             }
                            }else{
                                 c.SetMUNICIPIO_CLAVE(record.get(13).toUpperCase()); 
                            }
                            c.SetMUNICIPIO_NOMBRE(record.get(14).toUpperCase());
                            c.SetSUBCONTRATACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(15).toUpperCase()));
                            c.SetDESPIDO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(16).toUpperCase()));
                            c.SetRESCISION_RL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(17).toUpperCase()));
                            c.SetTERMINACION_RESCISION_RL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(18).toUpperCase()));
                            c.SetVIOLACION_CONTRATO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(19).toUpperCase()));
                            c.SetRIESGO_TRABAJO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(20).toUpperCase()));
                            c.SetREVISION_CONTRATO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(21).toUpperCase()));
                            c.SetPART_UTILIDADES(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(22).toUpperCase()));
                            c.SetOTRO_MOTIV_CONFLICTO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(23).toUpperCase()));
                            c.SetOTRO_ESP_CONFLICTO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(24).toUpperCase()));
                            c.SetCIRCUNS_MOTIVO_CONFL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(25).toUpperCase()));
                            c.SetDETERM_EMPLEO_EMBARAZO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(26).toUpperCase()));
                            c.SetDETERM_EMPLEO_EDAD(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(27).toUpperCase()));
                            c.SetDETERM_EMPLEO_GENERO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(28).toUpperCase()));
                            c.SetDETERM_EMPLEO_ORIEN_SEX(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(29).toUpperCase()));
                            c.SetDETERM_EMPLEO_DISCAPACIDAD(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(30).toUpperCase()));
                            c.SetDETERM_EMPLEO_SOCIAL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(31).toUpperCase()));
                            c.SetDETERM_EMPLEO_ORIGEN(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(32).toUpperCase()));
                            c.SetDETERM_EMPLEO_RELIGION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(33).toUpperCase()));
                            c.SetDETERM_EMPLEO_MIGRA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(34).toUpperCase()));
                            c.SetOTRO_DISCRIMINACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(35).toUpperCase()));
                            c.SetOTRO_ESP_DISCRIMI(record.get(36).toUpperCase());
                            c.SetTRATA_LABORAL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(37).toUpperCase()));
                            c.SetTRABAJO_FORZOSO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(38).toUpperCase()));
                            c.SetTRABAJO_INFANTIL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(39).toUpperCase()));
                            c.SetHOSTIGAMIENTO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(40).toUpperCase()));
                            c.SetACOSO_SEXUAL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(41).toUpperCase()));
                            c.SetPAGO_PRESTACIONES(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(42).toUpperCase()));
                            c.SetINDEMNIZACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(43).toUpperCase()));
                            c.SetREINSTALACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(44).toUpperCase()));
                            c.SetSALARIO_RETENIDO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(45).toUpperCase()));
                            c.SetAUMENTO_SALARIO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(46).toUpperCase()));
                            c.SetDERECHO_ASCENSO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(47).toUpperCase()));
                            c.SetDERECHO_PREFERENCIA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(48).toUpperCase()));
                            c.SetDERECHO_ANTIGUEDAD(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(49).toUpperCase()));
                            c.SetOTRO_CONCEPTO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(50).toUpperCase()));
                            c.SetOTRO_ESP_RECLAMADO(record.get(51).toUpperCase());
                            c.SetAGUINALDO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(52).toUpperCase()));
                            c.SetVACACIONES(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(53).toUpperCase()));
                            c.SetPRIMA_VACACIONAL(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(54).toUpperCase()));
                            c.SetPRIMA_ANTIGUEDAD(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(55).toUpperCase()));
                            c.SetOTRO_TIPO_PREST(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(56).toUpperCase()));
                            c.SetOTRO_ESP_PRESTAC(record.get(57).toUpperCase());
                            c.SetMOTIVO_CONFLICTO_COLECT(record.get(58).toUpperCase());
                            c.SetINCOMPETENCIA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(59).toUpperCase()));
                            c.SetTIPO_INCOMPETENCIA(conver.CON_V3_TC_TIPO_INCOMPETENCIAJL(con, record.get(60).toUpperCase()));
                            c.SetOTRO_ESP_INCOMP(record.get(61).toUpperCase());
                            c.SetFECHA_PRES_DEMANDA(conver.toH2Date(record.get(62).toUpperCase(),"FECHA_PRES_DEMANDA"));
                            c.SetCONSTANCIA_CONS_EXPEDIDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(63).toUpperCase()));
                            c.SetCONSTANCIA_CLAVE(record.get(64).toUpperCase());
                            c.SetASUN_EXCEP_CONCILIACION(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(65).toUpperCase()));
                            c.SetPREVE_DEMANDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(66).toUpperCase()));
                            c.SetDESAHOGO_PREV_DEMANDA(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(67).toUpperCase()));
                            c.SetESTATUS_DEMANDA(conver.CON_V3_TC_ESTATUS_DEMANDAJL(con, record.get(68).toUpperCase()));
                            c.SetCAU_IMP_ADM_DEMANDA(conver.CON_V3_TC_CAU_IMPI_ADMI_DEMJL(con, record.get(69).toUpperCase()));
                            c.SetFECHA_ADMI_DEMANDA(conver.toH2Date(record.get(70).toUpperCase(),"FECHA_ADMI_DEMANDA"));
                            c.SetCANTIDAD_ACTORES(record.get(71).toUpperCase());
                            c.SetCANTIDAD_DEMANDADOS(record.get(72).toUpperCase());
                            c.SetAUDIENCIA_PRELIM(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(73).toUpperCase()));
                            c.SetFECHA_AUDIENCIA_PRELIM(conver.toH2Date(record.get(74).toUpperCase(),"FECHA_AUDIENCIA_PRELIM"));
                            c.SetAUDIENCIA_JUICIO(conver.CON_V3_TC_RESPUESTA_SIMPLEJL(con, record.get(75).toUpperCase()));
                            c.SetFECHA_AUDIENCIA_JUICIO(conver.toH2Date(record.get(76).toUpperCase(),"FECHA_AUDIENCIA_JUICIO"));
                            c.SetESTATUS_EXPEDIENTE(conver.CON_V3_TC_ESTATUS_EXPEDIENTEJL(con, record.get(77).toUpperCase()));
                            c.SetFECHA_ACTO_PROCESAL(conver.toH2Date(record.get(78).toUpperCase(),"FECHA_ACTO_PROCESAL"));
                            c.SetFASE_SOLI_EXPEDIENTE(conver.CON_V3_TC_FASE_EXPEDIENTEJL(con, record.get(79).toUpperCase()));
                            c.SetFORMA_SOLUCIONFE(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(80).toUpperCase()));
                            c.SetOTRO_ESP_SOLUCIONFE(record.get(81).toUpperCase());
                            c.SetFECHA_DICTO_RESOLUCIONFE(conver.toH2Date(record.get(82).toUpperCase(),"FECHA_DICTO_RESOLUCIONFE"));
                            c.SetMONTO_SOLUCION_FE(record.get(83).toUpperCase());
                            c.SetFORMA_SOLUCIONAP(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(84).toUpperCase()));
                            c.SetOTRO_ESP_SOLUCIONAP(record.get(85).toUpperCase());
                            c.SetFECHA_DICTO_RESOLUCIONAP(conver.toH2Date(record.get(86).toUpperCase(),"FECHA_DICTO_RESOLUCIONAP"));
                            c.SetMONTO_SOLUCION_AP(record.get(87).toUpperCase());
                            c.SetFORMA_SOLUCIONAJ(conver.CON_V3_TC_FORMA_SOLUCIONJL(con, record.get(88).toUpperCase()));
                            c.SetOTRO_ESP_SOLUCIONAJ(record.get(89).toUpperCase());
                            c.SetFECHA_RESOLUCIONAJ(conver.toH2Date(record.get(90).toUpperCase(),"FECHA_RESOLUCIONAJ"));
                            c.SetTIPO_SENTENCIAAJ(conver.CON_V3_TC_TIPO_SENTENCIAJL(con, record.get(91).toUpperCase()));
                            c.SetMONTO_SOLUCIONAJ(record.get(92).toUpperCase());
                            c.SetCOMENTARIOS(record.get(93).toUpperCase());

                            ad.add(c);
                            
   /*                         System.out.println(
    "REG=" + TotalRegistros +
    " | CLAVE_ORGANO=" + record.get(1) +
    " | EXPEDIENTE=" + record.get(2) +
    " | SECTOR_RAW=" + record.get(9) +
    " | SUBSECTOR_RAW=" + record.get(10) +
    " | SECTOR_CONV=" + c.GetSECTOR_RAMA() +
    " | SUBSECTOR_CONV=" + c.GetSUBSECTOR_RAMA()
);*/
                        }
                        System.out.println("entro 1");
                        if (TotalRegistros > 0) {
                            if (Inserta == true) {
                                System.out.println("entro 2");
                                cargar.setVisible(true);
                                // OJO: AJUSTA los nombres reales de columnas en tu tabla H2
                                final String sql
                                        = "INSERT INTO V3_TR_ORDINARIOJL ("
                                        + "NOMBRE_ORGANO_JURIS, CLAVE_ORGANO, EXPEDIENTE_CLAVE, FECHA_APERTURA_EXPEDIENTE, TIPO_ASUNTO,\n"
                                        + "NAT_CONFLICTO, CONTRATO_ESCRITO, TIPO_CONTRATO, RAMA_INDUS_INVOLUCRADA, SECTOR_RAMA,\n"
                                        + "SUBSECTOR_RAMA, ENTIDAD_CLAVE, ENTIDAD_NOMBRE, MUNICIPIO_CLAVE, MUNICIPIO_NOMBRE,\n"
                                        + "SUBCONTRATACION, DESPIDO, RESCISION_RL, TERMINACION_RESCISION_RL, VIOLACION_CONTRATO,\n"
                                        + "RIESGO_TRABAJO, REVISION_CONTRATO, PART_UTILIDADES, OTRO_MOTIV_CONFLICTO, OTRO_ESP_CONFLICTO,\n"
                                        + "CIRCUNS_MOTIVO_CONFL, DETERM_EMPLEO_EMBARAZO, DETERM_EMPLEO_EDAD, DETERM_EMPLEO_GENERO, DETERM_EMPLEO_ORIEN_SEX,\n"
                                        + "DETERM_EMPLEO_DISCAPACIDAD, DETERM_EMPLEO_SOCIAL, DETERM_EMPLEO_ORIGEN, DETERM_EMPLEO_RELIGION, DETERM_EMPLEO_MIGRA,\n"
                                        + "OTRO_DISCRIMINACION, OTRO_ESP_DISCRIMI, TRATA_LABORAL, TRABAJO_FORZOSO, TRABAJO_INFANTIL,\n"
                                        + "HOSTIGAMIENTO, ACOSO_SEXUAL, PAGO_PRESTACIONES, INDEMNIZACION, REINSTALACION,\n"
                                        + "SALARIO_RETENIDO, AUMENTO_SALARIO, DERECHO_ASCENSO, DERECHO_PREFERENCIA, DERECHO_ANTIGUEDAD,\n"
                                        + "OTRO_CONCEPTO, OTRO_ESP_RECLAMADO, AGUINALDO, VACACIONES, PRIMA_VACACIONAL,\n"
                                        + "PRIMA_ANTIGUEDAD, OTRO_TIPO_PREST, OTRO_ESP_PRESTAC, MOTIVO_CONFLICTO_COLECT, INCOMPETENCIA,\n"
                                        + "TIPO_INCOMPETENCIA, OTRO_ESP_INCOMP, FECHA_PRES_DEMANDA, CONSTANCIA_CONS_EXPEDIDA, CONSTANCIA_CLAVE,\n"
                                        + "ASUN_EXCEP_CONCILIACION, PREVE_DEMANDA, DESAHOGO_PREV_DEMANDA, ESTATUS_DEMANDA, CAU_IMP_ADM_DEMANDA,\n"
                                        + "FECHA_ADMI_DEMANDA, CANTIDAD_ACTORES, CANTIDAD_DEMANDADOS, AUDIENCIA_PRELIM, FECHA_AUDIENCIA_PRELIM,\n"
                                        + "AUDIENCIA_JUICIO, FECHA_AUDIENCIA_JUICIO, ESTATUS_EXPEDIENTE, FECHA_ACTO_PROCESAL, FASE_SOLI_EXPEDIENTE,\n"
                                        + "FORMA_SOLUCIONFE, OTRO_ESP_SOLUCIONFE, FECHA_DICTO_RESOLUCIONFE, MONTO_SOLUCION_FE, FORMA_SOLUCIONAP,\n"
                                        + "OTRO_ESP_SOLUCIONAP, FECHA_DICTO_RESOLUCIONAP, MONTO_SOLUCION_AP, FORMA_SOLUCIONAJ, OTRO_ESP_SOLUCIONAJ,\n"
                                        + "FECHA_RESOLUCIONAJ, TIPO_SENTENCIAAJ, MONTO_SOLUCIONAJ, COMENTARIOS\n"
                                        + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
                                        Ordinario a = ad.get(i);
                                        // ✅ usa tu helper para setear params (mismo orden de tu INSERT)
                                        setParamsAudiencias(ps, a);
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
                                                insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, blockEnd, "V3_TR_ORDINARIOJL");

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
                                            insertarBloqueUnoAUno(con, conErr, ps, ad, blockStart, ad.size(), "V3_TR_ORDINARIOJL");
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
                            JOptionPane.showMessageDialog(null, "Archivo .CSV sin Registros-V3_TMP_AUDIENCIASJL");
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

    private static void guardarError(Connection conErr, String tablaDestino, Ordinario a,
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

    private static void setParamsAudiencias(PreparedStatement ps, Ordinario a) throws SQLException {
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
        ps.setString(17, a.GetDESPIDO());
        ps.setString(18, a.GetRESCISION_RL());
        ps.setString(19, a.GetTERMINACION_RESCISION_RL());
        ps.setString(20, a.GetVIOLACION_CONTRATO());

        ps.setString(21, a.GetRIESGO_TRABAJO());
        ps.setString(22, a.GetREVISION_CONTRATO());
        ps.setString(23, a.GetPART_UTILIDADES());
        ps.setString(24, a.GetOTRO_MOTIV_CONFLICTO());
        ps.setString(25, a.GetOTRO_ESP_CONFLICTO());

        ps.setString(26, a.GetCIRCUNS_MOTIVO_CONFL());
        ps.setString(27, a.GetDETERM_EMPLEO_EMBARAZO());
        ps.setString(28, a.GetDETERM_EMPLEO_EDAD());
        ps.setString(29, a.GetDETERM_EMPLEO_GENERO());
        ps.setString(30, a.GetDETERM_EMPLEO_ORIEN_SEX());

        ps.setString(31, a.GetDETERM_EMPLEO_DISCAPACIDAD());
        ps.setString(32, a.GetDETERM_EMPLEO_SOCIAL());
        ps.setString(33, a.GetDETERM_EMPLEO_ORIGEN());
        ps.setString(34, a.GetDETERM_EMPLEO_RELIGION());
        ps.setString(35, a.GetDETERM_EMPLEO_MIGRA());

        ps.setString(36, a.GetOTRO_DISCRIMINACION());
        ps.setString(37, a.GetOTRO_ESP_DISCRIMI());
        ps.setString(38, a.GetTRATA_LABORAL());
        ps.setString(39, a.GetTRABAJO_FORZOSO());
        ps.setString(40, a.GetTRABAJO_INFANTIL());

        ps.setString(41, a.GetHOSTIGAMIENTO());
        ps.setString(42, a.GetACOSO_SEXUAL());
        ps.setString(43, a.GetPAGO_PRESTACIONES());
        ps.setString(44, a.GetINDEMNIZACION());
        ps.setString(45, a.GetREINSTALACION());

        ps.setString(46, a.GetSALARIO_RETENIDO());
        ps.setString(47, a.GetAUMENTO_SALARIO());
        ps.setString(48, a.GetDERECHO_ASCENSO());
        ps.setString(49, a.GetDERECHO_PREFERENCIA());
        ps.setString(50, a.GetDERECHO_ANTIGUEDAD());

        ps.setString(51, a.GetOTRO_CONCEPTO());
        ps.setString(52, a.GetOTRO_ESP_RECLAMADO());
        ps.setString(53, a.GetAGUINALDO());
        ps.setString(54, a.GetVACACIONES());
        ps.setString(55, a.GetPRIMA_VACACIONAL());

        ps.setString(56, a.GetPRIMA_ANTIGUEDAD());
        ps.setString(57, a.GetOTRO_TIPO_PREST());
        ps.setString(58, a.GetOTRO_ESP_PRESTAC());
        ps.setString(59, a.GetMOTIVO_CONFLICTO_COLECT());
        ps.setString(60, a.GetINCOMPETENCIA());

        ps.setString(61, a.GetTIPO_INCOMPETENCIA());
        ps.setString(62, a.GetOTRO_ESP_INCOMP());
        ps.setString(63, a.GetFECHA_PRES_DEMANDA());
        ps.setString(64, a.GetCONSTANCIA_CONS_EXPEDIDA());
        ps.setString(65, a.GetCONSTANCIA_CLAVE());

        ps.setString(66, a.GetASUN_EXCEP_CONCILIACION());
        ps.setString(67, a.GetPREVE_DEMANDA());
        ps.setString(68, a.GetDESAHOGO_PREV_DEMANDA());
        ps.setString(69, a.GetESTATUS_DEMANDA());
        ps.setString(70, a.GetCAU_IMP_ADM_DEMANDA());

        ps.setString(71, a.GetFECHA_ADMI_DEMANDA());
        ps.setString(72, a.GetCANTIDAD_ACTORES());
        ps.setString(73, a.GetCANTIDAD_DEMANDADOS());
        ps.setString(74, a.GetAUDIENCIA_PRELIM());
        ps.setString(75, a.GetFECHA_AUDIENCIA_PRELIM());

        ps.setString(76, a.GetAUDIENCIA_JUICIO());
        ps.setString(77, a.GetFECHA_AUDIENCIA_JUICIO());
        ps.setString(78, a.GetESTATUS_EXPEDIENTE());
        ps.setString(79, a.GetFECHA_ACTO_PROCESAL());
        ps.setString(80, a.GetFASE_SOLI_EXPEDIENTE());

        ps.setString(81, a.GetFORMA_SOLUCIONFE());
        ps.setString(82, a.GetOTRO_ESP_SOLUCIONFE());
        ps.setString(83, a.GetFECHA_DICTO_RESOLUCIONFE());
        ps.setString(84, a.GetMONTO_SOLUCION_FE());
        ps.setString(85, a.GetFORMA_SOLUCIONAP());

        ps.setString(86, a.GetOTRO_ESP_SOLUCIONAP());
        ps.setString(87, a.GetFECHA_DICTO_RESOLUCIONAP());
        ps.setString(88, a.GetMONTO_SOLUCION_AP());
        ps.setString(89, a.GetFORMA_SOLUCIONAJ());
        ps.setString(90, a.GetOTRO_ESP_SOLUCIONAJ());

        ps.setString(91, a.GetFECHA_RESOLUCIONAJ());
        ps.setString(92, a.GetTIPO_SENTENCIAAJ());
        ps.setString(93, a.GetMONTO_SOLUCIONAJ());
        ps.setString(94, a.GetCOMENTARIOS());
    }

    private static void insertarBloqueUnoAUno(Connection con, Connection conErr, PreparedStatement ps,
            ArrayList<Ordinario> ad, int inicio, int fin, String tablaDestino) throws SQLException {

        for (int i = inicio; i < fin; i++) {
            Ordinario a = ad.get(i);

            try {
                setParamsAudiencias(ps, a);
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
