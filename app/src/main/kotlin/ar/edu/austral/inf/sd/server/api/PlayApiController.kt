package ar.edu.austral.inf.sd.server.api

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity

import org.springframework.web.bind.annotation.*
import org.springframework.validation.annotation.Validated
import org.springframework.beans.factory.annotation.Autowired

import jakarta.validation.Valid

@RestController
@Validated
@RequestMapping("\${api.base-path:}")
class PlayApiController(@Autowired(required = true) val service: PlayApiService) {

    @RequestMapping(
        method = [RequestMethod.POST],
        value = ["/play"],
        produces = ["application/json"]
    )
    fun sendMessage(@Valid @RequestBody body: String): ResponseEntity<Any> {
        return try {
            val response = service.sendMessage(body)
            ResponseEntity(response, HttpStatus.OK)
        } catch (e: GetawayTimeOutException) {
            val errorResponse = mapOf(
                "code" to "504",
                "error" to e.message,
            )
            ResponseEntity(errorResponse, HttpStatus.GATEWAY_TIMEOUT)
        } catch (e: ServiceUnavailableException) {
            val errorResponse = mapOf(
                "code" to "503",
                "error" to e.message,
            )
            ResponseEntity(errorResponse, HttpStatus.SERVICE_UNAVAILABLE)
        }
        catch (e: Exception)
        {
            val errorResponse = mapOf(
                "code" to "500",
                "error" to e.message,
            )
            ResponseEntity(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR)
        }

    }
}
