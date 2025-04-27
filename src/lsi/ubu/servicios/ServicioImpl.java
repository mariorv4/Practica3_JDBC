package lsi.ubu.servicios;

import java.sql.*;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;
import lsi.ubu.Misc;

public class ServicioImpl implements Servicio {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);
    private static final int DEFAULT_INVOICE_DAYS = 4;

    @Override
    public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
        PoolDeConexiones pool = PoolDeConexiones.getInstance();
        Connection con = null;
        PreparedStatement st = null;
        ResultSet rs = null;

        try {
            con = pool.getConnection();
            con.setAutoCommit(false);

            // 1. Comprobar existencia del cliente
            String sqlCliente = "SELECT 1 FROM Clientes WHERE NIF = ?";
            st = con.prepareStatement(sqlCliente);
            st.setString(1, nifCliente);
            rs = st.executeQuery();
            if (!rs.next()) {
                throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
            }
            rs.close();
            st.close();

            // 2. Comprobar existencia del vehículo
            String sqlVehiculo = "SELECT 1 FROM Vehiculos WHERE matricula = ?";
            st = con.prepareStatement(sqlVehiculo);
            st.setString(1, matricula);
            rs = st.executeQuery();
            if (!rs.next()) {
                throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
            }
            rs.close();
            st.close();

            // 3. Calcular fechaFin si es null
            if (fechaFin == null) {
                fechaFin = Misc.addDays(fechaIni, DEFAULT_INVOICE_DAYS);
            }

            // 4. Comprobar número de días
            int dias = Misc.howManyDaysBetween(fechaFin, fechaIni);
            if (dias < 1) {
                throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
            }

            // 5. Comprobar disponibilidad del vehículo (solape de fechas)
            // Un vehículo está ocupado si existe alguna reserva donde:
            // (fecha_ini <= fechaFin) AND (fecha_fin >= fechaIni)
            String sqlDisponibilidad = "SELECT 1 FROM Reservas WHERE matricula = ? AND fecha_ini <= ? AND fecha_fin >= ?";
            st = con.prepareStatement(sqlDisponibilidad);
            st.setString(1, matricula);
            st.setDate(2, new java.sql.Date(fechaFin.getTime()));
            st.setDate(3, new java.sql.Date(fechaIni.getTime()));
            rs = st.executeQuery();
            if (rs.next()) {
                throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO);
            }
            rs.close();
            st.close();

            // 6. Insertar la reserva
            // Asumimos que nroReserva es generado por la secuencia seq_reservas.NEXTVAL
            String sqlInsert = "INSERT INTO Reservas (nroReserva, cliente, matricula, fecha_ini, fecha_fin) VALUES (seq_reservas.NEXTVAL, ?, ?, ?, ?)";
            st = con.prepareStatement(sqlInsert);
            st.setString(1, nifCliente);
            st.setString(2, matricula);
            st.setDate(3, new java.sql.Date(fechaIni.getTime()));
            st.setDate(4, new java.sql.Date(fechaFin.getTime()));
            st.executeUpdate();

            con.commit();

        } catch (AlquilerCochesException ace) {
            if (con != null) con.rollback();
            throw ace;
        } catch (Exception e) {
            if (con != null) con.rollback();
            LOGGER.error("Error inesperado en alquiler", e);
            throw new SQLException(e);
        } finally {
            if (rs != null) try { rs.close(); } catch (Exception ignored) {}
            if (st != null) try { st.close(); } catch (Exception ignored) {}
            if (con != null) try { con.setAutoCommit(true); con.close(); } catch (Exception ignored) {}
        }
    }
}
