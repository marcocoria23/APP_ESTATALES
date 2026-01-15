package triggers;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreaTriggers {

    private void imprimirError(String trigger, SQLException e) {
        System.err.println("❌ Error creando trigger: " + trigger);
        System.err.println("Mensaje   : " + e.getMessage());
        System.err.println("SQLState  : " + e.getSQLState());
        System.err.println("ErrorCode : " + e.getErrorCode());
        e.printStackTrace();
    }

    public void crearTriggerAudiencias(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_AUDIENCIASJL_BI");
            st.execute("CREATE TRIGGER V3_TR_AUDIENCIASJL_BI BEFORE INSERT ON V3_TR_AUDIENCIASJL FOR EACH ROW CALL \"triggers.V3TrAudienciasJL\"");
            System.out.println("✔ V3_TR_AUDIENCIASJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_AUDIENCIASJL_BI", e);
        }
    }

    public void crearTriggerColectEconom(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_COLECT_ECONOMJL_BI");
            st.execute("CREATE TRIGGER V3_TR_COLECT_ECONOMJL_BI BEFORE INSERT ON V3_TR_COLECT_ECONOMJL FOR EACH ROW CALL \"triggers.V3TrColectEconomJL\"");
            System.out.println("✔ V3_TR_COLECT_ECONOMJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_COLECT_ECONOMJL_BI", e);
        }
    }

    public void crearTriggerColectivo(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_COLECTIVOJL_BI");
            st.execute("CREATE TRIGGER V3_TR_COLECTIVOJL_BI BEFORE INSERT ON V3_TR_COLECTIVOJL FOR EACH ROW CALL \"triggers.V3TrColectivoJL\"");
            System.out.println("✔ V3_TR_COLECTIVOJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_COLECTIVOJL_BI", e);
        }
    }

    public void crearTriggerControlExpediente(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_CONTROL_EXPEDIENTEJL_BI");
            st.execute("CREATE TRIGGER V3_TR_CONTROL_EXPEDIENTEJL_BI BEFORE INSERT ON V3_TR_CONTROL_EXPEDIENTEJL FOR EACH ROW CALL \"triggers.V3TrControlExpedienteJL\"");
            System.out.println("✔ V3_TR_CONTROL_EXPEDIENTEJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_CONTROL_EXPEDIENTEJL_BI", e);
        }
    }

    public void crearTriggerEjecucion(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_EJECUCIONJL_BI");
            st.execute("CREATE TRIGGER V3_TR_EJECUCIONJL_BI BEFORE INSERT ON V3_TR_EJECUCIONJL FOR EACH ROW CALL \"triggers.V3TrEjecucionJL\"");
            System.out.println("✔ V3_TR_EJECUCIONJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_EJECUCIONJL_BI", e);
        }
    }

    public void crearTriggerHuelga(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_HUELGAJL_BI");
            st.execute("CREATE TRIGGER V3_TR_HUELGAJL_BI BEFORE INSERT ON V3_TR_HUELGAJL FOR EACH ROW CALL \"triggers.V3TrHuelgaJL\"");
            System.out.println("✔ V3_TR_HUELGAJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_HUELGAJL_BI", e);
        }
    }

    public void crearTriggerIndividual(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_INDIVIDUALJL_BI");
            st.execute("CREATE TRIGGER V3_TR_INDIVIDUALJL_BI BEFORE INSERT ON V3_TR_INDIVIDUALJL FOR EACH ROW CALL \"triggers.V3TrIndividualJL\"");
            System.out.println("✔ V3_TR_INDIVIDUALJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_INDIVIDUALJL_BI", e);
        }
    }

    public void crearTriggerOrdinario(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_ORDINARIOJL_BI");
            st.execute("CREATE TRIGGER V3_TR_ORDINARIOJL_BI BEFORE INSERT ON V3_TR_ORDINARIOJL FOR EACH ROW CALL \"triggers.V3TrOrdinarioJL\"");
            System.out.println("✔ V3_TR_ORDINARIOJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_ORDINARIOJL_BI", e);
        }
    }

    public void crearTriggerParaprocesal(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PARAPROCESALJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PARAPROCESALJL_BI BEFORE INSERT ON V3_TR_PARAPROCESALJL FOR EACH ROW CALL \"triggers.V3TrParaprocesalJL\"");
            System.out.println("✔ V3_TR_PARAPROCESALJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PARAPROCESALJL_BI", e);
        }
    }

    public void crearTriggerPartActColectEconom(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_COLECT_ECONOMJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_ACT_COLECT_ECONOMJL_BI BEFORE INSERT ON V3_TR_PART_ACT_COLECT_ECONOMJL FOR EACH ROW CALL \"triggers.V3TrPartActColectEconomJL\"");
            System.out.println("✔ V3_TR_PART_ACT_COLECT_ECONOMJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_ACT_COLECT_ECONOMJL_BI", e);
        }
    }

    public void crearTriggerPartActColectivo(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_COLECTIVOJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_ACT_COLECTIVOJL_BI BEFORE INSERT ON V3_TR_PART_ACT_COLECTIVOJL FOR EACH ROW CALL \"triggers.V3TrPartActColectivoJL\"");
            System.out.println("✔ V3_TR_PART_ACT_COLECTIVOJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_ACT_COLECTIVOJL_BI", e);
        }
    }

    public void crearTriggerPartActHuelga(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_HUELGAJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_ACT_HUELGAJL_BI BEFORE INSERT ON V3_TR_PART_ACT_HUELGAJL FOR EACH ROW CALL \"triggers.V3TrPartActHuelgaJL\"");
            System.out.println("✔ V3_TR_PART_ACT_HUELGAJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_ACT_HUELGAJL_BI", e);
        }
    }

    public void crearTriggerPartActIndividual(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_INDIVIDUALJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_ACT_INDIVIDUALJL_BI BEFORE INSERT ON V3_TR_PART_ACT_INDIVIDUALJL FOR EACH ROW CALL \"triggers.V3TrPartActIndividualJL\"");
            System.out.println("✔ V3_TR_PART_ACT_INDIVIDUALJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_ACT_INDIVIDUALJL_BI", e);
        }
    }

    public void crearTriggerPartActOrdinario(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_ACT_ORDINARIOJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_ACT_ORDINARIOJL_BI BEFORE INSERT ON V3_TR_PART_ACT_ORDINARIOJL FOR EACH ROW CALL \"triggers.V3TrPartActOrdinarioJL\"");
            System.out.println("✔ V3_TR_PART_ACT_ORDINARIOJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_ACT_ORDINARIOJL_BI", e);
        }
    }

    public void crearTriggerPartDemColectEconom(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_COLECT_ECONOMJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_DEM_COLECT_ECONOMJL_BI BEFORE INSERT ON V3_TR_PART_DEM_COLECT_ECONOMJL FOR EACH ROW CALL \"triggers.V3TrPartDemColectEconomJL\"");
            System.out.println("✔ V3_TR_PART_DEM_COLECT_ECONOMJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_DEM_COLECT_ECONOMJL_BI", e);
        }
    }

    public void crearTriggerPartDemColectivo(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_COLECTIVOJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_DEM_COLECTIVOJL_BI BEFORE INSERT ON V3_TR_PART_DEM_COLECTIVOJL FOR EACH ROW CALL \"triggers.V3TrPartDemColectivoJL\"");
            System.out.println("✔ V3_TR_PART_DEM_COLECTIVOJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_DEM_COLECTIVOJL_BI", e);
        }
    }

    public void crearTriggerPartDemHuelga(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_HUELGAJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_DEM_HUELGAJL_BI BEFORE INSERT ON V3_TR_PART_DEM_HUELGAJL FOR EACH ROW CALL \"triggers.V3TrPartDemHuelgaJL\"");
            System.out.println("✔ V3_TR_PART_DEM_HUELGAJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_DEM_HUELGAJL_BI", e);
        }
    }

    public void crearTriggerPartDemIndividual(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_INDIVIDUALJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_DEM_INDIVIDUALJL_BI BEFORE INSERT ON V3_TR_PART_DEM_INDIVIDUALJL FOR EACH ROW CALL \"triggers.V3TrPartDemIndividualJL\"");
            System.out.println("✔ V3_TR_PART_DEM_INDIVIDUALJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_DEM_INDIVIDUALJL_BI", e);
        }
    }

    public void crearTriggerPartDemOrdinario(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PART_DEM_ORDINARIOJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PART_DEM_ORDINARIOJL_BI BEFORE INSERT ON V3_TR_PART_DEM_ORDINARIOJL FOR EACH ROW CALL \"triggers.V3TrPartDemOrdinarioJL\"");
            System.out.println("✔ V3_TR_PART_DEM_ORDINARIOJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PART_DEM_ORDINARIOJL_BI", e);
        }
    }

    public void crearTriggerPrefCredito(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_PREF_CREDITOJL_BI");
            st.execute("CREATE TRIGGER V3_TR_PREF_CREDITOJL_BI BEFORE INSERT ON V3_TR_PREF_CREDITOJL FOR EACH ROW CALL \"triggers.V3TrPreferenciaCreditoJL\"");
            System.out.println("✔ V3_TR_PREF_CREDITOJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_PREF_CREDITOJL_BI", e);
        }
    }

    public void crearTriggerTercerias(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS V3_TR_TERCERIASJL_BI");
            st.execute("CREATE TRIGGER V3_TR_TERCERIASJL_BI BEFORE INSERT ON V3_TR_TERCERIASJL FOR EACH ROW CALL \"triggers.V3TrTerceriasJL\"");
            System.out.println("✔ V3_TR_TERCERIASJL_BI");
        } catch (SQLException e) {
            imprimirError("V3_TR_TERCERIASJL_BI", e);
        }
    }
}
