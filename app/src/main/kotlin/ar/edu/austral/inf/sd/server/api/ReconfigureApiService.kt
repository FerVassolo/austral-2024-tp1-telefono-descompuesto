package ar.edu.austral.inf.sd.server.api


interface ReconfigureApiService {

    /**
     * POST /reconfigure
     * Cambio de siguiente participante
     *
     * @param uuid  (optional)
     * @param salt  (optional)
     * @param nextHost  (optional)
     * @param nextPort  (optional)
     * @param xGameTimestamp  (optional)
     * @return Cambio aceptado (status code 200)
     *         or Datos inválidos (status code 400)
     * @see ReconfigureApi#reconfigure
     */
    fun reconfigure(uuid: java.util.UUID?, salt: kotlin.String?, nextHost: kotlin.String?, nextPort: kotlin.Int?, xGameTimestamp: kotlin.Int?): kotlin.String
}
