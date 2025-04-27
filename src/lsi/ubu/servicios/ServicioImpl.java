package lsi.ubu.servicios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4;

	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		/*
		 * El calculo de los dias se da hecho
		 */
		long diasDiff = DIAS_DE_ALQUILER;
		if (fechaFin != null) {
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());

			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
		}

		try {
			con = pool.getConnection();

			/* A completar por el alumnado... */

			/* ================================= AYUDA R�PIDA ===========================*/
			/*
			 * Algunas de las columnas utilizan tipo numeric en SQL, lo que se traduce en
			 * BigDecimal para Java.
			 * 
			 * Convertir un entero en BigDecimal: new BigDecimal(diasDiff)
			 * 
			 * Sumar 2 BigDecimals: usar metodo "add" de la clase BigDecimal
			 * 
			 * Multiplicar 2 BigDecimals: usar metodo "multiply" de la clase BigDecimal
			 *
			 * 
			 * Paso de util.Date a sql.Date java.sql.Date sqlFechaIni = new
			 * java.sql.Date(sqlFechaIni.getTime());
			 *
			 *
			 * Recuerda que hay casos donde la fecha fin es nula, por lo que se debe de
			 * calcular sumando los dias de alquiler (ver variable DIAS_DE_ALQUILER) a la
			 * fecha ini.
			 */
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

		} catch (AlquilerCochesException ace) {
            // Completar por el alumno
            LOGGER.debug(ace.getMessage());
            throw ace;

        } catch (SQLException e) {
			// Completar por el alumno
			LOGGER.debug(e.getMessage());
			throw e;

		} finally {
			/* A rellenar por el alumnado*/
          
		}
	}
}