package ar.edu.austral.inf.sd.server.api

import ar.edu.austral.inf.sd.server.model.Signature
import ar.edu.austral.inf.sd.server.model.Signatures
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
class RelayApiController(@Autowired(required = true) val service: RelayApiService) {


    @RequestMapping(
        method = [RequestMethod.POST],
        value = ["/relay"],
        produces = ["application/json"],
        consumes = ["multipart/form-data"]
    )
    fun relayMessage( @RequestParam(value = "message", required = true) message: kotlin.String , @RequestPart(value = "signatures", required = true) signatures: Signatures , @RequestHeader(value = "X-Game-Timestamp", required = false) xGameTimestamp: kotlin.Int?): ResponseEntity<Signature> {
        return ResponseEntity(service.relayMessage(message, signatures, xGameTimestamp), HttpStatus.valueOf(200))
    }
}
