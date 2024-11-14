package ar.edu.austral.inf.sd.server.api

import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity

import org.springframework.web.bind.annotation.*
import org.springframework.validation.annotation.Validated
import org.springframework.web.context.request.NativeWebRequest
import org.springframework.beans.factory.annotation.Autowired

import jakarta.validation.Valid
import jakarta.validation.constraints.DecimalMax
import jakarta.validation.constraints.DecimalMin
import jakarta.validation.constraints.Email
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotNull
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size

import kotlin.collections.List
import kotlin.collections.Map

@RestController
@Validated
@RequestMapping("\${api.base-path:}")
class ReconfigureApiController(@Autowired(required = true) val service: ReconfigureApiService) {


    @RequestMapping(
        method = [RequestMethod.POST],
        value = ["/reconfigure"],
        produces = ["application/json"]
    )
    fun reconfigure( @Valid @RequestParam(value = "uuid", required = false) uuid: java.util.UUID?, @Valid @RequestParam(value = "salt", required = false) salt: kotlin.String?, @Valid @RequestParam(value = "nextHost", required = false) nextHost: kotlin.String?, @Valid @RequestParam(value = "nextPort", required = false) nextPort: kotlin.Int?, @RequestHeader(value = "X-Game-Timestamp", required = false) xGameTimestamp: kotlin.Int?): ResponseEntity<kotlin.String> {
        return ResponseEntity(service.reconfigure(uuid, salt, nextHost, nextPort, xGameTimestamp), HttpStatus.valueOf(200))
    }
}
