
package LeerQuery.beans;

import Bean_Procedures.Bean;
import java.sql.Connection;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

/**
 *
 * @author ALEJANDRO.CASTILLO
 */
public class BeanTR_EXPEDIENTE extends Bean implements SQLData  {
    
            private String SQLTypeName;
            private String ID_EXPEDIENTE;
            private String ID_TIPO_EXPEDIENTE;
            private String ID_ORGANOJ;
            private String CLAVE_EXPEDIENTE;
            private String CLAVE_CUADERNO_INCID;
            private String FECHA_PRESENT_INCIDENTE;
            private String FECHA_APERT_CUADER_INCID;
            private String ID_TIPO_INCIDENTE;
            private String FECHA_APERTURA_EXPED;
            private String ID_TIPO_ASUNTO;
            private String ID_NAT_CONFLICTO;
            private String PREG_TRAB_CONTR_ESCRI;
            private String ID_TIPO_CONTRATO;
            private String RAMA_INVOLUC;
            private String ID_SECTOR_SBTOR;
            private String FECHA_PRES_EJECU_SENT;
            private String ID_ENT_MPIO;
            private String PREG_OUTSOURCING;
            private String PREG_APL_CIR_VIN_MOT_CONF;
            private String PREG_PRESTACION;
            private String PREG_FECHA_CELEBR_AUDIENCIA;
            private String FECHA_AUDIENCIA;
            private String MOTIVO_CONF_COLECTIVO;
            private String PREG_INCOMPETENCIA;
            private String ID_TIPO_INCOMPETENCIA;
            private String FECHA_PLIEGO_PETICION;
            private String FECHA_PRESENT_DEMANDA;
            private String FECHA_PRESENT_PROMO;
            private String FECHA_ADMISION_PROMO;
            private String ID_PROMOVENTE;
            private String PREG_CONST_CONCILIAC;
            private String CLAVE_CONST_CONCILIAC;
            private String PREG_ASUNTO_VINC_CONCIL_P;
            private String PREG_FORMULO_DEMANDA;
            private String PREG_DESAHOGO_DEMANDA;
            private String ID_ESTATUS_DEMANDA;
            private String ID_CAUSA_IMP_ADM_DEMANDA;
            private String FECHA_ADM_DEMANDA;
            private String CANT_ACTORES;
            private String CANT_DEMANDADOS;
            private String PREG_HUELGA_EMPLAZAM;
            private String FECHA_HUELGA_EMPLAZAM;
            private String PREG_PREHUELGA;
            private String PREG_AUDIENCIA_CONCIL;
            private String FECHA_AUDIENCIA_CONCIL;
            private String PREG_HUELGA_ESTALLA;
            private String ID_HUELGA_LICITUD;
            private String ID_HUELGA_EXISTENCIA;
            private String PREG_AUTO_DEPURACION_TRAM;
            private String FECHA_AUTO_DEPURACION;
            private String PREG_AUD_PRELIM_CELEBR;
            private String FECHA_AUDIENCIA_PRELIM;
            private String PREG_CELEBRA_AUD_JUICIO;
            private String FECHA_AUD_JUICIO;
            private String PREG_AUDIENCIA_COL_NAT_ECO;
            private String FECHA_AUDIENCIA_COL_NAT_ECO;
            private String ID_ESTATUS_EXPED;
            private String FECHA_CONCLUY_EXPED;
            private String FECHA_ULT_ACT_PROC;
            private String ID_FASE_SOL_EXPED;
            private String FECHA_CONCL_EJECU;
            private String ID_FASE_CONCL_EJEC;
            private String ID_FORMA_SOLUCION;
            private String FECHA_DICTO_SOLUCION;
            private String PORCENT_SOLICITADO;
            private String PORCENT_OTORGADO;
            private String ID_TIPO_SENTENCIA;
            private String FECHA_HUELGA_ESTALLA;
            private String FECHA_HUELGA_LEVANTAM;
            private String HUELGA_DIAS;
            private String MONTO_SOLUCION;
            private String PORCENT_SALARIOS_CAIDOS;
            private String COMENTARIOS;
            private String PERIODO;

        
            public String GetID_EXPEDIENTE() {
              return ID_EXPEDIENTE;
            }

            public void SetID_EXPEDIENTE(String ID_EXPEDIENTE) {
                this.ID_EXPEDIENTE = ID_EXPEDIENTE;
            }


            public String GetID_TIPO_EXPEDIENTE() {
                return ID_TIPO_EXPEDIENTE;
            }

            public void SetID_TIPO_EXPEDIENTE(String ID_TIPO_EXPEDIENTE) {
                this.ID_TIPO_EXPEDIENTE = ID_TIPO_EXPEDIENTE;
            }

            public String GetID_ORGANOJ() {
                    return ID_ORGANOJ;
                }

                public void SetID_ORGANOJ(String ID_ORGANOJ) {
                    this.ID_ORGANOJ = ID_ORGANOJ;
                }
            public String GetCLAVE_EXPEDIENTE() {
                    return CLAVE_EXPEDIENTE;
                }

                public void SetCLAVE_EXPEDIENTE(String CLAVE_EXPEDIENTE) {
                    this.CLAVE_EXPEDIENTE = CLAVE_EXPEDIENTE;
                }
            public String GetCLAVE_CUADERNO_INCID() {
                    return CLAVE_CUADERNO_INCID;
                }

                public void SetCLAVE_CUADERNO_INCID(String CLAVE_CUADERNO_INCID) {
                    this.CLAVE_CUADERNO_INCID = CLAVE_CUADERNO_INCID;
                }
            public String GetFECHA_PRESENT_INCIDENTE() {
                    return FECHA_PRESENT_INCIDENTE;
                }

                public void SetFECHA_PRESENT_INCIDENTE(String FECHA_PRESENT_INCIDENTE) {
                    this.FECHA_PRESENT_INCIDENTE = FECHA_PRESENT_INCIDENTE;
                }
            public String GetFECHA_APERT_CUADER_INCID() {
                    return FECHA_APERT_CUADER_INCID;
                }

                public void SetFECHA_APERT_CUADER_INCID(String FECHA_APERT_CUADER_INCID) {
                    this.FECHA_APERT_CUADER_INCID = FECHA_APERT_CUADER_INCID;
                }
            public String GetID_TIPO_INCIDENTE() {
                    return ID_TIPO_INCIDENTE;
                }

                public void SetID_TIPO_INCIDENTE(String ID_TIPO_INCIDENTE) {
                    this.ID_TIPO_INCIDENTE = ID_TIPO_INCIDENTE;
                }
            public String GetFECHA_APERTURA_EXPED() {
                    return FECHA_APERTURA_EXPED;
                }

                public void SetFECHA_APERTURA_EXPED(String FECHA_APERTURA_EXPED) {
                    this.FECHA_APERTURA_EXPED = FECHA_APERTURA_EXPED;
                }
            public String GetID_TIPO_ASUNTO() {
                    return ID_TIPO_ASUNTO;
                }

                public void SetID_TIPO_ASUNTO(String ID_TIPO_ASUNTO) {
                    this.ID_TIPO_ASUNTO = ID_TIPO_ASUNTO;
                }
            public String GetID_NAT_CONFLICTO() {
                    return ID_NAT_CONFLICTO;
                }

                public void SetID_NAT_CONFLICTO(String ID_NAT_CONFLICTO) {
                    this.ID_NAT_CONFLICTO = ID_NAT_CONFLICTO;
                }
            public String GetPREG_TRAB_CONTR_ESCRI() {
                    return PREG_TRAB_CONTR_ESCRI;
                }

                public void SetPREG_TRAB_CONTR_ESCRI(String PREG_TRAB_CONTR_ESCRI) {
                    this.PREG_TRAB_CONTR_ESCRI = PREG_TRAB_CONTR_ESCRI;
                }
            public String GetID_TIPO_CONTRATO() {
                    return ID_TIPO_CONTRATO;
                }

                public void SetID_TIPO_CONTRATO(String ID_TIPO_CONTRATO) {
                    this.ID_TIPO_CONTRATO = ID_TIPO_CONTRATO;
                }
            public String GetRAMA_INVOLUC() {
                    return RAMA_INVOLUC;
                }

                public void SetRAMA_INVOLUC(String RAMA_INVOLUC) {
                    this.RAMA_INVOLUC = RAMA_INVOLUC;
                }
            public String GetID_SECTOR_SBTOR() {
                    return ID_SECTOR_SBTOR;
                }

                public void SetID_SECTOR_SBTOR(String ID_SECTOR_SBTOR) {
                    this.ID_SECTOR_SBTOR = ID_SECTOR_SBTOR;
                }
            public String GetFECHA_PRES_EJECU_SENT() {
                    return FECHA_PRES_EJECU_SENT;
                }

                public void SetFECHA_PRES_EJECU_SENT(String FECHA_PRES_EJECU_SENT) {
                    this.FECHA_PRES_EJECU_SENT = FECHA_PRES_EJECU_SENT;
                }
            public String GetID_ENT_MPIO() {
                    return ID_ENT_MPIO;
                }

                public void SetID_ENT_MPIO(String ID_ENT_MPIO) {
                    this.ID_ENT_MPIO = ID_ENT_MPIO;
                }
            public String GetPREG_OUTSOURCING() {
                    return PREG_OUTSOURCING;
                }

                public void SetPREG_OUTSOURCING(String PREG_OUTSOURCING) {
                    this.PREG_OUTSOURCING = PREG_OUTSOURCING;
                }
            public String GetPREG_APL_CIR_VIN_MOT_CONF() {
                    return PREG_APL_CIR_VIN_MOT_CONF;
                }

                public void SetPREG_APL_CIR_VIN_MOT_CONF(String PREG_APL_CIR_VIN_MOT_CONF) {
                    this.PREG_APL_CIR_VIN_MOT_CONF = PREG_APL_CIR_VIN_MOT_CONF;
                }
            public String GetPREG_PRESTACION() {
                    return PREG_PRESTACION;
                }

                public void SetPREG_PRESTACION(String PREG_PRESTACION) {
                    this.PREG_PRESTACION = PREG_PRESTACION;
                }
            public String GetPREG_FECHA_CELEBR_AUDIENCIA() {
                    return PREG_FECHA_CELEBR_AUDIENCIA;
                }

                public void SetPREG_FECHA_CELEBR_AUDIENCIA(String PREG_FECHA_CELEBR_AUDIENCIA) {
                    this.PREG_FECHA_CELEBR_AUDIENCIA = PREG_FECHA_CELEBR_AUDIENCIA;
                }
            public String GetFECHA_AUDIENCIA() {
                    return FECHA_AUDIENCIA;
                }

                public void SetFECHA_AUDIENCIA(String FECHA_AUDIENCIA) {
                    this.FECHA_AUDIENCIA = FECHA_AUDIENCIA;
                }
            public String GetMOTIVO_CONF_COLECTIVO() {
                    return MOTIVO_CONF_COLECTIVO;
                }

                public void SetMOTIVO_CONF_COLECTIVO(String MOTIVO_CONF_COLECTIVO) {
                    this.MOTIVO_CONF_COLECTIVO = MOTIVO_CONF_COLECTIVO;
                }
            public String GetPREG_INCOMPETENCIA() {
                    return PREG_INCOMPETENCIA;
                }

                public void SetPREG_INCOMPETENCIA(String PREG_INCOMPETENCIA) {
                    this.PREG_INCOMPETENCIA = PREG_INCOMPETENCIA;
                }
            public String GetID_TIPO_INCOMPETENCIA() {
                    return ID_TIPO_INCOMPETENCIA;
                }

                public void SetID_TIPO_INCOMPETENCIA(String ID_TIPO_INCOMPETENCIA) {
                    this.ID_TIPO_INCOMPETENCIA = ID_TIPO_INCOMPETENCIA;
                }
            public String GetFECHA_PLIEGO_PETICION() {
                    return FECHA_PLIEGO_PETICION;
                }

                public void SetFECHA_PLIEGO_PETICION(String FECHA_PLIEGO_PETICION) {
                    this.FECHA_PLIEGO_PETICION = FECHA_PLIEGO_PETICION;
                }
            public String GetFECHA_PRESENT_DEMANDA() {
                    return FECHA_PRESENT_DEMANDA;
                }

                public void SetFECHA_PRESENT_DEMANDA(String FECHA_PRESENT_DEMANDA) {
                    this.FECHA_PRESENT_DEMANDA = FECHA_PRESENT_DEMANDA;
                }
            public String GetFECHA_PRESENT_PROMO() {
                    return FECHA_PRESENT_PROMO;
                }

                public void SetFECHA_PRESENT_PROMO(String FECHA_PRESENT_PROMO) {
                    this.FECHA_PRESENT_PROMO = FECHA_PRESENT_PROMO;
                }
            public String GetFECHA_ADMISION_PROMO() {
                    return FECHA_ADMISION_PROMO;
                }

                public void SetFECHA_ADMISION_PROMO(String FECHA_ADMISION_PROMO) {
                    this.FECHA_ADMISION_PROMO = FECHA_ADMISION_PROMO;
                }
            public String GetID_PROMOVENTE() {
                    return ID_PROMOVENTE;
                }

                public void SetID_PROMOVENTE(String ID_PROMOVENTE) {
                    this.ID_PROMOVENTE = ID_PROMOVENTE;
                }
            public String GetPREG_CONST_CONCILIAC() {
                    return PREG_CONST_CONCILIAC;
                }

                public void SetPREG_CONST_CONCILIAC(String PREG_CONST_CONCILIAC) {
                    this.PREG_CONST_CONCILIAC = PREG_CONST_CONCILIAC;
                }
            public String GetCLAVE_CONST_CONCILIAC() {
                    return CLAVE_CONST_CONCILIAC;
                }

                public void SetCLAVE_CONST_CONCILIAC(String CLAVE_CONST_CONCILIAC) {
                    this.CLAVE_CONST_CONCILIAC = CLAVE_CONST_CONCILIAC;
                }
            public String GetPREG_ASUNTO_VINC_CONCIL_P() {
                    return PREG_ASUNTO_VINC_CONCIL_P;
                }

                public void SetPREG_ASUNTO_VINC_CONCIL_P(String PREG_ASUNTO_VINC_CONCIL_P) {
                    this.PREG_ASUNTO_VINC_CONCIL_P = PREG_ASUNTO_VINC_CONCIL_P;
                }
            public String GetPREG_FORMULO_DEMANDA() {
                    return PREG_FORMULO_DEMANDA;
                }

                public void SetPREG_FORMULO_DEMANDA(String PREG_FORMULO_DEMANDA) {
                    this.PREG_FORMULO_DEMANDA = PREG_FORMULO_DEMANDA;
                }
            public String GetPREG_DESAHOGO_DEMANDA() {
                    return PREG_DESAHOGO_DEMANDA;
                }

                public void SetPREG_DESAHOGO_DEMANDA(String PREG_DESAHOGO_DEMANDA) {
                    this.PREG_DESAHOGO_DEMANDA = PREG_DESAHOGO_DEMANDA;
                }
            public String GetID_ESTATUS_DEMANDA() {
                    return ID_ESTATUS_DEMANDA;
                }

                public void SetID_ESTATUS_DEMANDA(String ID_ESTATUS_DEMANDA) {
                    this.ID_ESTATUS_DEMANDA = ID_ESTATUS_DEMANDA;
                }
            public String GetID_CAUSA_IMP_ADM_DEMANDA() {
                    return ID_CAUSA_IMP_ADM_DEMANDA;
                }

                public void SetID_CAUSA_IMP_ADM_DEMANDA(String ID_CAUSA_IMP_ADM_DEMANDA) {
                    this.ID_CAUSA_IMP_ADM_DEMANDA = ID_CAUSA_IMP_ADM_DEMANDA;
                }
            public String GetFECHA_ADM_DEMANDA() {
                    return FECHA_ADM_DEMANDA;
                }

                public void SetFECHA_ADM_DEMANDA(String FECHA_ADM_DEMANDA) {
                    this.FECHA_ADM_DEMANDA = FECHA_ADM_DEMANDA;
                }
            public String GetCANT_ACTORES() {
                    return CANT_ACTORES;
                }

                public void SetCANT_ACTORES(String CANT_ACTORES) {
                    this.CANT_ACTORES = CANT_ACTORES;
                }
            public String GetCANT_DEMANDADOS() {
                    return CANT_DEMANDADOS;
                }

                public void SetCANT_DEMANDADOS(String CANT_DEMANDADOS) {
                    this.CANT_DEMANDADOS = CANT_DEMANDADOS;
                }
            public String GetPREG_HUELGA_EMPLAZAM() {
                    return PREG_HUELGA_EMPLAZAM;
                }

                public void SetPREG_HUELGA_EMPLAZAM(String PREG_HUELGA_EMPLAZAM) {
                    this.PREG_HUELGA_EMPLAZAM = PREG_HUELGA_EMPLAZAM;
                }
            public String GetFECHA_HUELGA_EMPLAZAM() {
                    return FECHA_HUELGA_EMPLAZAM;
                }

                public void SetFECHA_HUELGA_EMPLAZAM(String FECHA_HUELGA_EMPLAZAM) {
                    this.FECHA_HUELGA_EMPLAZAM = FECHA_HUELGA_EMPLAZAM;
                }
            public String GetPREG_PREHUELGA() {
                    return PREG_PREHUELGA;
                }

                public void SetPREG_PREHUELGA(String PREG_PREHUELGA) {
                    this.PREG_PREHUELGA = PREG_PREHUELGA;
                }
            public String GetPREG_AUDIENCIA_CONCIL() {
                    return PREG_AUDIENCIA_CONCIL;
                }

                public void SetPREG_AUDIENCIA_CONCIL(String PREG_AUDIENCIA_CONCIL) {
                    this.PREG_AUDIENCIA_CONCIL = PREG_AUDIENCIA_CONCIL;
                }
            public String GetFECHA_AUDIENCIA_CONCIL() {
                    return FECHA_AUDIENCIA_CONCIL;
                }

                public void SetFECHA_AUDIENCIA_CONCIL(String FECHA_AUDIENCIA_CONCIL) {
                    this.FECHA_AUDIENCIA_CONCIL = FECHA_AUDIENCIA_CONCIL;
                }
            public String GetPREG_HUELGA_ESTALLA() {
                    return PREG_HUELGA_ESTALLA;
                }

                public void SetPREG_HUELGA_ESTALLA(String PREG_HUELGA_ESTALLA) {
                    this.PREG_HUELGA_ESTALLA = PREG_HUELGA_ESTALLA;
                }
            public String GetID_HUELGA_LICITUD() {
                    return ID_HUELGA_LICITUD;
                }

                public void SetID_HUELGA_LICITUD(String ID_HUELGA_LICITUD) {
                    this.ID_HUELGA_LICITUD = ID_HUELGA_LICITUD;
                }
            public String GetID_HUELGA_EXISTENCIA() {
                    return ID_HUELGA_EXISTENCIA;
                }

                public void SetID_HUELGA_EXISTENCIA(String ID_HUELGA_EXISTENCIA) {
                    this.ID_HUELGA_EXISTENCIA = ID_HUELGA_EXISTENCIA;
                }
            public String GetPREG_AUTO_DEPURACION_TRAM() {
                    return PREG_AUTO_DEPURACION_TRAM;
                }

                public void SetPREG_AUTO_DEPURACION_TRAM(String PREG_AUTO_DEPURACION_TRAM) {
                    this.PREG_AUTO_DEPURACION_TRAM = PREG_AUTO_DEPURACION_TRAM;
                }
            public String GetFECHA_AUTO_DEPURACION() {
                    return FECHA_AUTO_DEPURACION;
                }

                public void SetFECHA_AUTO_DEPURACION(String FECHA_AUTO_DEPURACION) {
                    this.FECHA_AUTO_DEPURACION = FECHA_AUTO_DEPURACION;
                }
            public String GetPREG_AUD_PRELIM_CELEBR() {
                    return PREG_AUD_PRELIM_CELEBR;
                }

                public void SetPREG_AUD_PRELIM_CELEBR(String PREG_AUD_PRELIM_CELEBR) {
                    this.PREG_AUD_PRELIM_CELEBR = PREG_AUD_PRELIM_CELEBR;
                }
            public String GetFECHA_AUDIENCIA_PRELIM() {
                    return FECHA_AUDIENCIA_PRELIM;
                }

                public void SetFECHA_AUDIENCIA_PRELIM(String FECHA_AUDIENCIA_PRELIM) {
                    this.FECHA_AUDIENCIA_PRELIM = FECHA_AUDIENCIA_PRELIM;
                }
            public String GetPREG_CELEBRA_AUD_JUICIO() {
                    return PREG_CELEBRA_AUD_JUICIO;
                }

                public void SetPREG_CELEBRA_AUD_JUICIO(String PREG_CELEBRA_AUD_JUICIO) {
                    this.PREG_CELEBRA_AUD_JUICIO = PREG_CELEBRA_AUD_JUICIO;
                }
            public String GetFECHA_AUD_JUICIO() {
                    return FECHA_AUD_JUICIO;
                }

                public void SetFECHA_AUD_JUICIO(String FECHA_AUD_JUICIO) {
                    this.FECHA_AUD_JUICIO = FECHA_AUD_JUICIO;
                }
            public String GetPREG_AUDIENCIA_COL_NAT_ECO() {
                    return PREG_AUDIENCIA_COL_NAT_ECO;
                }

                public void SetPREG_AUDIENCIA_COL_NAT_ECO(String PREG_AUDIENCIA_COL_NAT_ECO) {
                    this.PREG_AUDIENCIA_COL_NAT_ECO = PREG_AUDIENCIA_COL_NAT_ECO;
                }
            public String GetFECHA_AUDIENCIA_COL_NAT_ECO() {
                    return FECHA_AUDIENCIA_COL_NAT_ECO;
                }

                public void SetFECHA_AUDIENCIA_COL_NAT_ECO(String FECHA_AUDIENCIA_COL_NAT_ECO) {
                    this.FECHA_AUDIENCIA_COL_NAT_ECO = FECHA_AUDIENCIA_COL_NAT_ECO;
                }
            public String GetID_ESTATUS_EXPED() {
                    return ID_ESTATUS_EXPED;
                }

                public void SetID_ESTATUS_EXPED(String ID_ESTATUS_EXPED) {
                    this.ID_ESTATUS_EXPED = ID_ESTATUS_EXPED;
                }
            public String GetFECHA_CONCLUY_EXPED() {
                    return FECHA_CONCLUY_EXPED;
                }

                public void SetFECHA_CONCLUY_EXPED(String FECHA_CONCLUY_EXPED) {
                    this.FECHA_CONCLUY_EXPED = FECHA_CONCLUY_EXPED;
                }
            public String GetFECHA_ULT_ACT_PROC() {
                    return FECHA_ULT_ACT_PROC;
                }

                public void SetFECHA_ULT_ACT_PROC(String FECHA_ULT_ACT_PROC) {
                    this.FECHA_ULT_ACT_PROC = FECHA_ULT_ACT_PROC;
                }
            public String GetID_FASE_SOL_EXPED() {
                    return ID_FASE_SOL_EXPED;
                }

                public void SetID_FASE_SOL_EXPED(String ID_FASE_SOL_EXPED) {
                    this.ID_FASE_SOL_EXPED = ID_FASE_SOL_EXPED;
                }
            public String GetFECHA_CONCL_EJECU() {
                    return FECHA_CONCL_EJECU;
                }

                public void SetFECHA_CONCL_EJECU(String FECHA_CONCL_EJECU) {
                    this.FECHA_CONCL_EJECU = FECHA_CONCL_EJECU;
                }
            public String GetID_FASE_CONCL_EJEC() {
                    return ID_FASE_CONCL_EJEC;
                }

                public void SetID_FASE_CONCL_EJEC(String ID_FASE_CONCL_EJEC) {
                    this.ID_FASE_CONCL_EJEC = ID_FASE_CONCL_EJEC;
                }
            public String GetID_FORMA_SOLUCION() {
                    return ID_FORMA_SOLUCION;
                }

                public void SetID_FORMA_SOLUCION(String ID_FORMA_SOLUCION) {
                    this.ID_FORMA_SOLUCION = ID_FORMA_SOLUCION;
                }
            public String GetFECHA_DICTO_SOLUCION() {
                    return FECHA_DICTO_SOLUCION;
                }

                public void SetFECHA_DICTO_SOLUCION(String FECHA_DICTO_SOLUCION) {
                    this.FECHA_DICTO_SOLUCION = FECHA_DICTO_SOLUCION;
                }


                public String GetPORCENT_SOLICITADO() {
            return PORCENT_SOLICITADO;
            }

            public void SetPORCENT_SOLICITADO(String PORCENT_SOLICITADO) {
            this.PORCENT_SOLICITADO = PORCENT_SOLICITADO;
            }

            public String GetPORCENT_OTORGADO() {
            return PORCENT_OTORGADO;
            }

            public void SetPORCENT_OTORGADO(String PORCENT_OTORGADO) {
            this.PORCENT_OTORGADO = PORCENT_OTORGADO;
            }

            public String GetID_TIPO_SENTENCIA() {
            return ID_TIPO_SENTENCIA;
            }

            public void SetID_TIPO_SENTENCIA(String ID_TIPO_SENTENCIA) {
            this.ID_TIPO_SENTENCIA = ID_TIPO_SENTENCIA;
            }
            public String GetFECHA_HUELGA_ESTALLA() {
            return FECHA_HUELGA_ESTALLA;
            }

            public void SetFECHA_HUELGA_ESTALLA(String FECHA_HUELGA_ESTALLA) {
            this.FECHA_HUELGA_ESTALLA = FECHA_HUELGA_ESTALLA;
            }
            public String GetFECHA_HUELGA_LEVANTAM() {
            return FECHA_HUELGA_LEVANTAM;
            }

            public void SetFECHA_HUELGA_LEVANTAM(String FECHA_HUELGA_LEVANTAM) {
            this.FECHA_HUELGA_LEVANTAM = FECHA_HUELGA_LEVANTAM;
            }
            public String GetHUELGA_DIAS() {
            return HUELGA_DIAS;
            }

            public void SetHUELGA_DIAS(String HUELGA_DIAS) {
            this.HUELGA_DIAS = HUELGA_DIAS;
            }
            public String GetMONTO_SOLUCION() {
            return MONTO_SOLUCION;
            }

            public void SetMONTO_SOLUCION(String MONTO_SOLUCION) {
            this.MONTO_SOLUCION = MONTO_SOLUCION;
            }
            public String GetPORCENT_SALARIOS_CAIDOS() {
            return PORCENT_SALARIOS_CAIDOS;
            }

            public void SetPORCENT_SALARIOS_CAIDOS(String PORCENT_SALARIOS_CAIDOS) {
            this.PORCENT_SALARIOS_CAIDOS = PORCENT_SALARIOS_CAIDOS;
            }
            public String GetCOMENTARIOS() {
            return COMENTARIOS;
            }

            public void SetCOMENTARIOS(String COMENTARIOS) {
            this.COMENTARIOS = COMENTARIOS;
            }
            public String GetPERIODO() {
            return PERIODO;
            }

            public void SetPERIODO(String PERIODO) {
            this.PERIODO = PERIODO;
            }




   

    @Override
    public Object[] getLlave() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public void setAtributos(Object[] obj) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void actualizaDesc(Connection conn) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

   
    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
    @Override
    public String getSQLTypeName() throws SQLException {
       return SQLTypeName;
    }

    @Override
    public Object[] toArray() {
       Object[] array = {
                    GetID_EXPEDIENTE(),
                    GetID_TIPO_EXPEDIENTE(),
                    GetID_ORGANOJ(),
                    GetCLAVE_EXPEDIENTE(),
                    GetCLAVE_CUADERNO_INCID(),
                    GetFECHA_PRESENT_INCIDENTE(),
                    GetFECHA_APERT_CUADER_INCID(),
                    GetID_TIPO_INCIDENTE(),
                    GetFECHA_APERTURA_EXPED(),
                    GetID_TIPO_ASUNTO(),
                    GetID_NAT_CONFLICTO(),
                    GetPREG_TRAB_CONTR_ESCRI(),
                    GetID_TIPO_CONTRATO(),
                    GetRAMA_INVOLUC(),
                    GetID_SECTOR_SBTOR(),
                    GetFECHA_PRES_EJECU_SENT(),
                    GetID_ENT_MPIO(),
                    GetPREG_OUTSOURCING(),
                    GetPREG_APL_CIR_VIN_MOT_CONF(),
                    GetPREG_PRESTACION(),
                    GetPREG_FECHA_CELEBR_AUDIENCIA(),
                    GetFECHA_AUDIENCIA(),
                    GetMOTIVO_CONF_COLECTIVO(),
                    GetPREG_INCOMPETENCIA(),
                    GetID_TIPO_INCOMPETENCIA(),
                    GetFECHA_PLIEGO_PETICION(),
                    GetFECHA_PRESENT_DEMANDA(),
                    GetFECHA_PRESENT_PROMO(),
                    GetFECHA_ADMISION_PROMO(),
                    GetID_PROMOVENTE(),
                    GetPREG_CONST_CONCILIAC(),
                    GetCLAVE_CONST_CONCILIAC(),
                    GetPREG_ASUNTO_VINC_CONCIL_P(),
                    GetPREG_FORMULO_DEMANDA(),
                    GetPREG_DESAHOGO_DEMANDA(),
                    GetID_ESTATUS_DEMANDA(),
                    GetID_CAUSA_IMP_ADM_DEMANDA(),
                    GetFECHA_ADM_DEMANDA(),
                    GetCANT_ACTORES(),
                    GetCANT_DEMANDADOS(),
                    GetPREG_HUELGA_EMPLAZAM(),
                    GetFECHA_HUELGA_EMPLAZAM(),
                    GetPREG_PREHUELGA(),
                    GetPREG_AUDIENCIA_CONCIL(),
                    GetFECHA_AUDIENCIA_CONCIL(),
                    GetPREG_HUELGA_ESTALLA(),
                    GetID_HUELGA_LICITUD(),
                    GetID_HUELGA_EXISTENCIA(),
                    GetPREG_AUTO_DEPURACION_TRAM(),
                    GetFECHA_AUTO_DEPURACION(),
                    GetPREG_AUD_PRELIM_CELEBR(),
                    GetFECHA_AUDIENCIA_PRELIM(),
                    GetPREG_CELEBRA_AUD_JUICIO(),
                    GetFECHA_AUD_JUICIO(),
                    GetPREG_AUDIENCIA_COL_NAT_ECO(),
                    GetFECHA_AUDIENCIA_COL_NAT_ECO(),
                    GetID_ESTATUS_EXPED(),
                    GetFECHA_CONCLUY_EXPED(),
                    GetFECHA_ULT_ACT_PROC(),
                    GetID_FASE_SOL_EXPED(),
                    GetFECHA_CONCL_EJECU(),
                    GetID_FASE_CONCL_EJEC(),
                    GetID_FORMA_SOLUCION(),
                    GetFECHA_DICTO_SOLUCION(),
                    GetPORCENT_SOLICITADO(),
                    GetPORCENT_OTORGADO(),
                    GetID_TIPO_SENTENCIA(),
                    GetFECHA_HUELGA_ESTALLA(),
                    GetFECHA_HUELGA_LEVANTAM(),
                    GetHUELGA_DIAS(),
                    GetMONTO_SOLUCION(),
                    GetPORCENT_SALARIOS_CAIDOS(),
                    GetCOMENTARIOS(),
                    GetPERIODO()


        };
        return array;
    }
    
     private String ots(Object obj) {
        String val = null;
        if (obj != null) {
            val = obj.toString();
        }
        return val;
    }

    private Integer oti(Object obj) {
        Integer num = null;
        if (obj != null) {
            try {
                num = (Integer) obj;
            } catch (Exception ex) {
                System.out.println("Error en casteo de numero [" + obj + "]");
            }
        }
        return num;
    }

    
}
