package ar.edu.austral.inf.sd

import ar.edu.austral.inf.sd.server.api.*
import ar.edu.austral.inf.sd.server.model.Node
import ar.edu.austral.inf.sd.server.model.PlayResponse
import ar.edu.austral.inf.sd.server.model.RegisterResponse
import ar.edu.austral.inf.sd.server.model.Signature
import ar.edu.austral.inf.sd.server.model.Signatures
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.flow.update
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClientException
import org.springframework.web.client.RestTemplate
import org.springframework.web.client.postForEntity
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.system.exitProcess

@Component
class ApiServicesImpl @Autowired constructor(
    private val restTemplate: RestTemplate
): RegisterNodeApiService, RelayApiService, PlayApiService, ReconfigureApiService, UnregisterNodeApiService {

    private var timeOuts = 0

    private val nodes: MutableList<Node> = mutableListOf()

    private var nextNode: RegisterResponse? = null
    private val messageDigest = MessageDigest.getInstance("SHA-512")
    private val salt = Base64.getUrlEncoder().encodeToString(Random.nextBytes(9))
    private val uuid = newUUID()
    private var myTimeout = -1


    private var myTimestamp: Int = 0
    private var nextNodeAfterNextTimestamp: RegisterResponse? = null

    private val currentRequest
        get() = (RequestContextHolder.getRequestAttributes() as ServletRequestAttributes).request
    private var resultReady = CountDownLatch(1)
    private var currentMessageWaiting = MutableStateFlow<PlayResponse?>(null)
    private var currentMessageResponse = MutableStateFlow<PlayResponse?>(null)
    private var xGameTimestamp = 0

    @Value("\${server.name:nada}")
    private val myServerName: String = ""
    @Value("\${server.host:localhost}")
    private val myServerHost: String = "localhost"
    @Value("\${server.port:8080}")
    private val myServerPort: Int = 0
    @Value("\${server.timeout:20}")
    private val timeout: Int = 20
    @Value("\${register.host:}")
    var registerHost: String = ""
    @Value("\${register.port:-1}")
    var registerPort: Int = -1


    // --- REGISTER
    override fun registerNode(host: String?, port: Int?, uuid: UUID?, salt: String?, name: String?): ResponseEntity<RegisterResponse> {
        println("Received salt: $salt")
        validateSalt(salt)

        val existingNode = nodes.find { it.uuid == uuid }
        if (existingNode != null) {
            return handleExistingNode(existingNode, salt)
        }

        val nextNode = determineNextNode()
        val newNode = createNode(host, port, uuid, salt, name)
        nodes.add(newNode)

        return ResponseEntity(
            RegisterResponse(nextNode.nextHost, nextNode.nextPort, timeout, xGameTimestamp),
            HttpStatus.OK
        )
    }

    private fun validateSalt(salt: String?) {
        try {
            Base64.getUrlDecoder().decode(salt)
        } catch (e: IllegalArgumentException) {
            throw BadRequestException("Could not decode salt as Base64")
        }
    }

    private fun handleExistingNode(existingNode: Node, salt: String?): ResponseEntity<RegisterResponse> {
        if (existingNode.salt == salt) {
            val nextNodeIndex = nodes.indexOf(existingNode) - 1
            val nextNode = nodes[nextNodeIndex]
            return ResponseEntity(
                RegisterResponse(nextNode.host, nextNode.port, timeout, xGameTimestamp),
                HttpStatus.ACCEPTED
            )
        } else {
            throw UnauthorizedException("Invalid salt")
        }
    }

    private fun determineNextNode(): RegisterResponse {
        return if (nodes.isEmpty()) {
            val me = RegisterResponse(myServerHost, myServerPort, timeout, xGameTimestamp)
            val meNode = Node(myServerHost, myServerPort, myServerName, uuid, salt)
            nodes.add(meNode)
            me
        } else {
            val lastNode = nodes.last()
            RegisterResponse(lastNode.host, lastNode.port, timeout, xGameTimestamp)
        }
    }

    private fun createNode(host: String?, port: Int?, uuid: UUID?, salt: String?, name: String?): Node {
        if (host == null || port == null || uuid == null || salt == null || name == null) {
            throw IllegalArgumentException("Node parameters cannot be null")
        }
        return Node(host, port, name, uuid, salt)
    }


    // --- RELAY
    override fun relayMessage(message: String, signatures: Signatures, xGameTimestamp: Int?): Signature {
        val receivedHash = calculateReceivedHash(message)
        val receivedContentType = getReceivedContentType()
        val receivedLength = message.length

        if (nextNode != null) {
            handleRelay(message, signatures, receivedContentType, xGameTimestamp)
        } else {
            handleFinalNode(message, signatures, receivedHash, receivedContentType, receivedLength)
        }

        return Signature(
            name = myServerName,
            hash = receivedHash,
            contentType = receivedContentType,
            contentLength = receivedLength
        )
    }

    private fun calculateReceivedHash(message: String): String {
        return doHash(message.encodeToByteArray(), salt)
    }

    private fun getReceivedContentType(): String {
        return currentRequest.getPart("message")?.contentType ?: "nada"
    }

    private fun handleRelay(message: String, signatures: Signatures, receivedContentType: String, xGameTimestamp: Int?) {
        val updatedSignatures = signatures.items + clientSign(message, receivedContentType)
        sendRelayMessage(message, receivedContentType, nextNode!!, Signatures(updatedSignatures), xGameTimestamp!!)
    }

    private fun handleFinalNode(message: String, signatures: Signatures, receivedHash: String, receivedContentType: String, receivedLength: Int) {
        if (currentMessageWaiting.value == null) {
            throw BadRequestException("no waiting message")
        }
        val current = currentMessageWaiting.getAndUpdate { null }!!
        val response = validatePlayResult(current, receivedHash, receivedLength, receivedContentType, signatures)
        currentMessageResponse.update { response }
        xGameTimestamp += 1
        resultReady.countDown()
    }

    // --- PLAY / SEND MESSAGE
    override fun sendMessage(body: String): PlayResponse {
        if (timeOuts >= 10) throw BadRequestException("The game is closed")

        initializeFirstNodeIfNeeded();

        val contentType = currentRequest.contentType
        val expectedSignatures = createExpectedSignatures(body, nodes, contentType)

        processRelayMessage(body, contentType)

        if (currentMessageResponse.value == null){
            timeOuts += 1
            throw GetawayTimeOutException("Last relay was not received on time")
        }

        if (doHash(body.encodeToByteArray(),  salt) != currentMessageResponse.value!!.receivedHash){
            throw ServiceUnavailableException("The hash or message received is not the same as the one sent")
        }

        if (!compareSignatures(expectedSignatures, currentMessageResponse.value!!.signatures)){
            throw InternalServerErrorException("There are missing signatures")
        }

        return currentMessageResponse.value!!
    }

    private fun processRelayMessage(body: String, contentType: String){
        currentMessageWaiting.update { newResponse(body) }
        sendRelayMessage(body, contentType, toRegisterResponse(nodes.last(), -1), Signatures(listOf()), xGameTimestamp)
        resultReady.await(timeout.toLong(), TimeUnit.SECONDS)
        resultReady = CountDownLatch(1)
    }

    private fun initializeFirstNodeIfNeeded() {
        if (nodes.isEmpty()) {
            val me = Node(myServerHost, myServerPort, myServerName, uuid, salt)
            nodes.add(me)
        }
    }

    private fun createExpectedSignatures(body: String, nodes: List<Node>, contentType:String): Signatures {

        val signatures = mutableListOf<Signature>()
        for (i in 1..< nodes.size){
            val node = nodes[i]
            val hash = doHash(body.encodeToByteArray(), node.salt)
            signatures.add(Signature(node.name, hash, contentType, body.length))
        }

        return Signatures(signatures)
    }

    private fun compareSignatures(a: Signatures, b: Signatures): Boolean{
        val aSignatureList = a.items
        val bSignatureList = b.items.reversed()

        if (aSignatureList.size != bSignatureList.size) return false

        for (i in aSignatureList.indices) {
            if (aSignatureList[i].hash != bSignatureList[i].hash) return false
        }

        return true
    }

    // --- REGISTER TO SERVER
    internal fun registerToServer(registerHost: String, registerPort: Int) {
        println("My salt: $salt")
        val url = buildRegisterUrl(registerHost, registerPort)

        try {
            val registerNodeResponse = sendRegisterRequest(url)
            handleRegisterResponse(registerNodeResponse)
        } catch (e: RestClientException) {
            registrationError(url, e)
        }
    }

    private fun buildRegisterUrl(registerHost: String, registerPort: Int): String {
        val registerUrl = "http://$registerHost:$registerPort/register-node"
        val registerParams = "?host=localhost&port=$myServerPort&name=$myServerName&uuid=$uuid&salt=$salt&name=$myServerName"
        return registerUrl + registerParams
    }

    private fun sendRegisterRequest(url: String): RegisterResponse {
        val response = restTemplate.postForEntity<RegisterResponse>(url)
        return response.body ?: throw IllegalStateException("Response body is null")
    }

    private fun handleRegisterResponse(registerNodeResponse: RegisterResponse) {
        println("nextNode = $registerNodeResponse")
        myTimestamp = registerNodeResponse.xGameTimestamp
        myTimeout = registerNodeResponse.timeout
        nextNode = RegisterResponse(
            nextHost = registerNodeResponse.nextHost,
            nextPort = registerNodeResponse.nextPort,
            timeout = registerNodeResponse.timeout,
            xGameTimestamp = registerNodeResponse.xGameTimestamp
        )
    }

    private fun registrationError(url: String, e: RestClientException) {
        println("Could not register to: $url")
        println("Error: ${e.message}")
        println("Shutting down")
        exitProcess(1)
    }


    // SendRelayMessage
    private fun sendRelayMessage(body: String, contentType: String, relayNode: RegisterResponse, signatures: Signatures, timestamp: Int) {
        validateTimestamp(timestamp)

        val nextNodeUrl = determineNextNodeUrl(relayNode, timestamp)

        val request = createRelayRequest(body, contentType, signatures, timestamp)

        try {
            postMessageToNode(nextNodeUrl, request)
        } catch (e: RestClientException) {
            handleRelayFailure(request, nextNodeUrl)
        }

        updateTimestamp(timestamp)
    }

    private fun validateTimestamp(timestamp: Int) {
        if (timestamp < myTimestamp) {
            throw BadRequestException("Invalid timestamp")
        }
    }

    private fun determineNextNodeUrl(relayNode: RegisterResponse, timestamp: Int): String {
        return if (nextNodeAfterNextTimestamp != null && timestamp >= nextNodeAfterNextTimestamp!!.xGameTimestamp) {
            myTimestamp = nextNodeAfterNextTimestamp!!.xGameTimestamp
            nextNode = nextNodeAfterNextTimestamp
            nextNodeAfterNextTimestamp = null
            "http://${nextNode!!.nextHost}:${nextNode!!.nextPort}/relay"
        } else {
            "http://${relayNode.nextHost}:${relayNode.nextPort}/relay"
        }
    }

    private fun createRelayRequest(body: String, contentType: String, signatures: Signatures, timestamp: Int): HttpEntity<LinkedMultiValueMap<String, Any>> {
        val messageHeaders = HttpHeaders().apply { setContentType(MediaType.parseMediaType(contentType)) }
        val messagePart = HttpEntity(body, messageHeaders)

        val signatureHeaders = HttpHeaders().apply { setContentType(MediaType.APPLICATION_JSON) }
        val signaturesPart = HttpEntity(signatures, signatureHeaders)

        val bodyParts = LinkedMultiValueMap<String, Any>().apply {
            add("message", messagePart)
            add("signatures", signaturesPart)
        }

        val requestHeaders = HttpHeaders().apply {
            setContentType(MediaType.MULTIPART_FORM_DATA)
            add("X-Game-Timestamp", timestamp.toString())
        }

        return HttpEntity(bodyParts, requestHeaders)
    }

    private fun postMessageToNode(nextNodeUrl: String, request: HttpEntity<LinkedMultiValueMap<String, Any>>) {
        restTemplate.postForEntity<Map<String, Any>>(nextNodeUrl, request)
    }

    private fun handleRelayFailure(request: HttpEntity<LinkedMultiValueMap<String, Any>>, nextNodeUrl: String) {
        val hostUrl = "http://${registerHost}:${registerPort}/relay"
        restTemplate.postForEntity<Map<String, Any>>(hostUrl, request)
        throw ServiceUnavailableException("Could not relay message to next node: $nextNodeUrl")
    }

    private fun updateTimestamp(timestamp: Int) {
        myTimestamp = timestamp
    }

    private fun clientSign(message: String, contentType: String): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), salt)
        return Signature(myServerName, receivedHash, contentType, message.length)
    }

    private fun newResponse(body: String) = PlayResponse(
        "Unknown",
        currentRequest.contentType,
        body.length,
        doHash(body.encodeToByteArray(), salt),
        "Unknown",
        -1,
        "N/A",
        Signatures(listOf())
    )

    private fun doHash(body: ByteArray, salt: String): String {
        val saltBytes = Base64.getUrlDecoder().decode(salt)
        messageDigest.update(saltBytes)
        val digest = messageDigest.digest(body)
        return Base64.getEncoder().encodeToString(digest)
    }

    private fun toRegisterResponse(node: Node, timestamp: Int): RegisterResponse {
        return RegisterResponse(
            node.host,
            node.port,
            timeout,
            timestamp
        )
    }

    private fun validatePlayResult(current: PlayResponse, receivedHash: String, receivedLength: Int, receivedContentType: String, signatures: Signatures): PlayResponse {
        return current.copy(
            contentResult = if (receivedHash == current.originalHash) "Success" else "Failure",
            receivedHash = receivedHash,
            receivedLength = receivedLength,
            receivedContentType = receivedContentType,
            signatures = signatures
        )
    }

    companion object {
        fun newUUID(): UUID = UUID.randomUUID()
    }

    override fun reconfigure(
        uuid: UUID?,
        salt: String?,
        nextHost: String?,
        nextPort: Int?,
        xGameTimestamp: Int?
    ): String {
        if (uuid != this.uuid || salt != this.salt){
            throw BadRequestException("Invalid data. The UUID or salt is incorrect")
        }

        nextNodeAfterNextTimestamp = RegisterResponse(nextHost!!, nextPort!!, timeout, xGameTimestamp!!)
        return "Reconfigured node ${this.uuid}"
    }

    // --- UNREGISTER
    override fun unregisterNode(uuid: UUID?, salt: String?): String {
        val nodeToUnregister = findNodeByUuid(uuid)
        validateNode(nodeToUnregister, uuid, salt)
        val nodeToUnregisterIndex = nodes.indexOf(nodeToUnregister)

        if (nodeToUnregisterIndex < nodes.size - 1) {
            reconfigureAdjacentNodes(nodeToUnregisterIndex)
        }

        nodes.removeAt(nodeToUnregisterIndex)
        return "Unregistered node $uuid"
    }

    private fun findNodeByUuid(uuid: UUID?): Node {
        return nodes.find { it.uuid == uuid!! } ?: throw NotFoundException("Node not found. UUID: $uuid")
    }

    private fun validateNode(node: Node, uuid: UUID?, salt: String?) {
        if (node.salt != salt) {
            throw BadRequestException("Invalid data. The salt is incorrect")
        }
    }

    private fun reconfigureAdjacentNodes(nodeIndex: Int) {
        val previousNode = nodes[nodeIndex + 1]
        val nextNode = nodes[nodeIndex - 1]

        val reconfigureUrl = buildReconfigureUrl(previousNode, nextNode)
        val request = buildReconfigureRequest()

        try {
            restTemplate.postForEntity<String>(reconfigureUrl, request)
        } catch (e: RestClientException) {
            print("Could not reconfigure to: $reconfigureUrl")
            throw e
        }
    }

    private fun buildReconfigureUrl(previousNode: Node, nextNode: Node): String {
        val baseUrl = "http://${previousNode.host}:${previousNode.port}/reconfigure"
        val params = "?uuid=${previousNode.uuid}&salt=${previousNode.salt}&nextHost=${nextNode.host}&nextPort=${nextNode.port}"
        return baseUrl + params
    }

    private fun buildReconfigureRequest(): HttpEntity<*> {
        val requestHeaders = HttpHeaders().apply {
            add("X-Game-Timestamp", xGameTimestamp.toString())
        }
        return HttpEntity(null, requestHeaders)
    }


    @PreDestroy
    fun onDestroy(){
        if (registerPort == -1) return // Coordinator just dies

        val unregisterUrl = "http://$registerHost:$registerPort/unregister-node"
        val unregisterParams = "?uuid=$uuid&salt=$salt"
        val url = unregisterUrl + unregisterParams

        try {
            restTemplate.postForEntity<String>(url)
        } catch (e: RestClientException){
            print("Could not unregister to: $url")
            throw e
        }
    }
}
