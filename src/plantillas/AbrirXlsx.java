/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package plantillas;

import java.awt.Desktop;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import javax.swing.JOptionPane;

/**
 *
 * @author ANTONIO.CORIA
 */
public class AbrirXlsx {
     public static void abrirPlantillaDesdeResources(String resourcePath) {
        try (InputStream in = AbrirXlsx.class.getResourceAsStream(resourcePath)) {

            if (in == null) {
                JOptionPane.showMessageDialog(null,
                        "No se encontró el recurso: " + resourcePath,
                        "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }

            // Crear archivo temporal (se abre con Excel)
            Path temp = Files.createTempFile("plantilla_", ".xlsx");
            temp.toFile().deleteOnExit();

            // Copiar el recurso al archivo temporal
            Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);

            // Abrir con la app asociada (Excel)
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().open(temp.toFile());
            } else {
                JOptionPane.showMessageDialog(null,
                        "Desktop no soportado en este entorno.",
                        "Error", JOptionPane.ERROR_MESSAGE);
            }

        } catch (Exception e) {
            JOptionPane.showMessageDialog(null,
                    "No se pudo abrir el archivo: " + e.getMessage(),
                    "Error", JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
    }
}