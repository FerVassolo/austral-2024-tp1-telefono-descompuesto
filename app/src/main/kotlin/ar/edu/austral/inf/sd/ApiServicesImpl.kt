package ar.edu.austral.inf.sd

import ar.edu.austral.inf.sd.server.api.*
import ar.edu.austral.inf.sd.server.model.PlayResponse
import ar.edu.austral.inf.sd.server.model.RegisterResponse
import ar.edu.austral.inf.sd.server.model.Signature
import ar.edu.austral.inf.sd.server.model.Signatures
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.flow.update
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.random.Random
import ar.edu.austral.inf.sd.server.model.Node
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClientException
import org.springframework.web.client.postForEntity
import java.util.concurrent.TimeUnit
import jakarta.annotation.PreDestroy


@Component
class ApiServicesImpl(
    private val restTemplate: RestTemplate
):  RegisterNodeApiService, RelayApiService, PlayApiService, ReconfigureApiService, UnregisterNodeApiService {

    @Value("\${server.name:nada}")
    private val serverName: String = ""


    @Value("\${server.port:8080}")
    private val serverPort: Int = 0
    private val nodes: MutableList<Node> = mutableListOf()
    private var nextNode: RegisterResponse? = null
    private val messageDigest = MessageDigest.getInstance("SHA-512")
    private val salt = newSalt()
    private val currentRequest
        get() = (RequestContextHolder.getRequestAttributes() as ServletRequestAttributes).request
    private var resultReady = CountDownLatch(1)
    private var currentMessageWaiting = MutableStateFlow<PlayResponse?>(null)
    private var currentMessageResponse = MutableStateFlow<PlayResponse?>(null)
    private var xGameTimestamp: Int = 0

    @Value("\${server.host:localhost}")
    private val serverHost: String = "localhost"
    @Value("\${timeout:20}")
    private val timeout: Int = 20
    @Value("\${register.host:}")
    var registerHost: String = ""
    @Value("\${register.port:-1}")
    var registerPort: Int = -1

    private val UUID = newUUID()

    private var timestamp: Int = 0
    private var nextTimestamp: Int? = null
    private var nextNodeAfterNextTimestamp: RegisterResponse? = null


    override fun registerNode(host: String?, port: Int?, uuid: UUID?, salt: String?, name: String?): RegisterResponse {
        println("Received salt: $salt")
        validateSalt(salt)

        val existingNode = findExistingNode(uuid)
        if (existingNode != null) {
            return handleExistingNode(existingNode, salt)
        }

        val nextNode = determineNextNode()
        val newNode = createNewNode(host, port, uuid, salt, name)
        nodes.add(newNode)

        return RegisterResponse(nextNode.nextHost, nextNode.nextPort, timeout, this.xGameTimestamp)
    }

    private fun validateSalt(salt: String?) {
        try {
            Base64.getUrlDecoder().decode(salt)
        } catch (e: IllegalArgumentException) {
            throw BadRequestException("Could not decode salt $salt")
        }
    }

    private fun findExistingNode(uuid: UUID?): Node? {
        return nodes.find { it.uuid == uuid }
    }

    private fun handleExistingNode(existingNode: Node, salt: String?): RegisterResponse {
        if (existingNode.salt == salt) {
            val nextNodeIndex = nodes.indexOf(existingNode) - 1
            val nextNode = nodes[nextNodeIndex]
            return RegisterResponse(nextNode.host, nextNode.port, timeout, this.xGameTimestamp)
        } else {
            throw Exception("Invalid salt")
        }
    }

    private fun determineNextNode(): RegisterResponse {
        return if (nodes.isEmpty()) {
            // Es el primer nodo
            val me = RegisterResponse(serverHost, serverPort, timeout, this.xGameTimestamp)
            val meNode = Node(serverHost, serverPort, serverName, UUID, this.salt)
            nodes.add(meNode)
            me
        } else {
            val lastNode = nodes.last()
            RegisterResponse(lastNode.host, lastNode.port, timeout, this.xGameTimestamp)
        }
    }

    private fun createNewNode(host: String?, port: Int?, uuid: UUID?, salt: String?, name: String?): Node {
        return Node(host!!, port!!, name!!, uuid!!, salt!!)
    }



    override fun relayMessage(message: String, signatures: Signatures, givenXGameTimestamp: Int?): Signature {
        val receivedHash = calculateHash(message)
        val receivedContentType = getMessageContentType()
        val receivedLength = message.length

        if (nextNode != null) {
            relayToNextNode(message, signatures, receivedContentType, givenXGameTimestamp!!)
        } else {
            processReceivedMessage(receivedHash, receivedLength, receivedContentType, signatures)
        }

        return Signature(
            name = serverName,
            hash = receivedHash,
            contentType = receivedContentType,
            contentLength = receivedLength
        )
    }

    private fun calculateHash(message: String): String {
        return doHash(message.encodeToByteArray(), salt)
    }

    private fun getMessageContentType(): String {
        return currentRequest.getPart("message")?.contentType ?: "nada"
    }

    private fun relayToNextNode(message: String, signatures: Signatures, contentType: String, timestamp: Int) {
        val newSignatures = signatures.items + clientSign(message, contentType)
        sendRelayMessage(message, contentType, nextNode!!, Signatures(newSignatures), timestamp)
    }

    private fun processReceivedMessage(receivedHash: String, receivedLength: Int, contentType: String, signatures: Signatures) {
        if (currentMessageWaiting.value == null) throw BadRequestException("There is no message to relay")
        val current = currentMessageWaiting.getAndUpdate { null }!!
        val response = validateResult(current, receivedHash, receivedLength, contentType, signatures)
        currentMessageResponse.update { response }
        this.xGameTimestamp += 1
        resultReady.countDown()
    }


    override fun sendMessage(body: String): PlayResponse {
        initializeNodeIfNeeded()
        currentMessageWaiting.update { newResponse(body) }
        val contentType = currentRequest.contentType
        val expectedSignatures = generateExpectedSignatures(body, nodes, contentType)

        sendRelayMessage(body, contentType, toRegisterResponse(nodes.last()), Signatures(listOf()), xGameTimestamp)
        awaitResultReady()

        validateResponse(expectedSignatures)

        return currentMessageResponse.value!!
    }

    private fun initializeNodeIfNeeded() {
        if (nodes.isEmpty()) {
            val me = Node(serverHost, serverPort, serverName, UUID, salt)
            nodes.add(me)
        }
    }

    private fun awaitResultReady() {
        resultReady.await(timeout.toLong(), TimeUnit.SECONDS)
        resultReady = CountDownLatch(1)

        if (currentMessageWaiting.value != null) {
            throw Exception("Timeout: relay was not recieved on time")
        }
    }

    private fun validateResponse(expectedSignatures: Signatures) {
        currentMessageResponse.value?.let {
            if (!compareSignatures(expectedSignatures, it.signatures)) {
                throw Exception("Received different signatures than expected")
            }
            if (currentMessageWaiting.value!!.originalHash != currentMessageWaiting.value!!.receivedHash) {
                throw Exception("Received different hash than expected")
            }
        } ?: throw Exception("No response received")
    }


    override fun unregisterNode(uuid: UUID?, salt: String?): String {
        val nodeToUnregister = findNodeByUuid(uuid)
        validateNodeData(nodeToUnregister, salt)
        val nodeToUnregisterIndex = nodes.indexOf(nodeToUnregister)

        if (shouldReconfigureNodes(nodeToUnregisterIndex)) {
            reconfigureAdjacentNodes(nodeToUnregisterIndex)
        }

        nodes.removeAt(nodeToUnregisterIndex)
        return "Unregister Successful"
    }

    private fun findNodeByUuid(uuid: UUID?): Node {
        return nodes.find { it.uuid == uuid } ?: throw Exception("Node with uuid: $uuid not found")
    }

    private fun validateNodeData(node: Node, salt: String?) {
        if (node.salt != salt) {
            throw BadRequestException("Invalid data")
        }
    }

    private fun shouldReconfigureNodes(index: Int): Boolean {
        return index < nodes.size - 1
    }

    private fun reconfigureAdjacentNodes(index: Int) {
        val previousNode = nodes[index + 1]
        val nextNode = nodes[index - 1]
        val url = buildReconfigureUrl(previousNode, nextNode)
        val requestHeaders = HttpHeaders().apply {
            add("X-Game-Timestamp", xGameTimestamp.toString())
        }
        val request = HttpEntity(requestHeaders.toMap())

        try {
            restTemplate.postForEntity<String>(url, request)
        } catch (e: RestClientException) {
            print("Could not reconfigure to: $url")
            throw e
        }
    }

    private fun buildReconfigureUrl(previousNode: Node, nextNode: Node): String {
        val reconfigureUrl = "http://${previousNode.host}:${previousNode.port}/reconfigure"
        val reconfigureParams = "?uuid=${previousNode.uuid}&salt=${previousNode.salt}&nextHost=${nextNode.host}&nextPort=${nextNode.port}"
        return reconfigureUrl + reconfigureParams
    }


    override fun reconfigure(
        uuid: UUID?,
        salt: String?,
        nextHost: String?,
        nextPort: Int?,
        xGameTimestamp: Int?
    ): String {
        if (uuid != UUID || salt != this.salt){
            throw BadRequestException("Invalid data")
        }
        nextNodeAfterNextTimestamp = RegisterResponse(nextHost!!, nextPort!!, timeout, xGameTimestamp!!)
        nextTimestamp = xGameTimestamp
        return "Reconfigured node $UUID"
    }

    internal fun registerToServer(registerHost: String, registerPort: Int) {
        val registerUrl = "http://$registerHost:$registerPort/register-node"
        val registerParams = "?host=localhost&port=$serverPort&name=$serverName&uuid=$UUID&salt=$salt&name=$serverName"
        val url = registerUrl + registerParams
        try {
            val response = restTemplate.postForEntity<RegisterResponse>(url)
            val registerNodeResponse: RegisterResponse = response.body!!
            println("nextNode = $registerNodeResponse")
            nextNode = with(registerNodeResponse) { RegisterResponse(nextHost, nextPort, timeout, xGameTimestamp) }
        } catch (e: RestClientException){
            print("Could not register to: $registerUrl")
        }
    }

    private fun clientSign(message: String, contentType: String): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), salt)
        return Signature(serverName, receivedHash, contentType, message.length)
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
        val saltBytes = Base64.getDecoder().decode(salt)
        messageDigest.update(saltBytes)
        val digest = messageDigest.digest(body)
        return Base64.getEncoder().encodeToString(digest)
    }

    companion object {
        fun newSalt(): String = Base64.getEncoder().encodeToString(Random.nextBytes(9))
        fun newUUID(): UUID = UUID.randomUUID()
    }



    private fun validateResult(current: PlayResponse, receivedHash: String, receivedLength: Int, receivedContentType: String, signatures: Signatures): PlayResponse {
        return current.copy(
            contentResult = if (receivedHash == current.originalHash) "Success" else "Failure",
            receivedHash = receivedHash,
            receivedLength = receivedLength,
            receivedContentType = receivedContentType,
            signatures = signatures
        )
    }

    private fun sendRelayMessage(
        body: String,
        contentType: String,
        relayNode: RegisterResponse,
        signatures: Signatures,
        timestamp: Int
    ) {
        validateTimestamp(timestamp)
        updateTimestampAndNodeIfNeeded(timestamp)
        val request = buildRelayRequest(body, contentType, signatures, timestamp)
        try {
            sendToNextNode(relayNode, request)
        } catch (e: RestClientException) {
            handleRelayFailure(request)
        }
        this.timestamp = timestamp
    }

    private fun validateTimestamp(timestamp: Int) {
        if (timestamp < this.timestamp) {
            throw BadRequestException("Invalid timestamp")
        }
    }

    private fun updateTimestampAndNodeIfNeeded(timestamp: Int) {
        if (nextTimestamp != null && timestamp >= nextTimestamp!!) {
            this.timestamp = nextTimestamp as Int
            nextNode = nextNodeAfterNextTimestamp
            nextTimestamp = null
            nextNodeAfterNextTimestamp = null
        }
    }

    private fun buildRelayRequest(
        body: String,
        contentType: String,
        signatures: Signatures,
        timestamp: Int
    ): HttpEntity<LinkedMultiValueMap<String, Any>> {
        val messageHeaders = HttpHeaders().apply {
            setContentType(MediaType.parseMediaType(contentType))
        }
        val messagePart = HttpEntity(body, messageHeaders)

        val signatureHeaders = HttpHeaders().apply {
            setContentType(MediaType.APPLICATION_JSON)
        }
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

    private fun sendToNextNode(relayNode: RegisterResponse, request: HttpEntity<LinkedMultiValueMap<String, Any>>) {
        val nextNodeUrl = "http://${relayNode.nextHost}:${relayNode.nextPort}/relay"
        restTemplate.postForEntity<Map<String, Any>>(nextNodeUrl, request)
    }

    private fun handleRelayFailure(request: HttpEntity<LinkedMultiValueMap<String, Any>>) {
        val hostUrl = "http://${registerHost}:${registerPort}/relay"
        restTemplate.postForEntity<Map<String, Any>>(hostUrl, request)
        throw Exception("Could not relay to next node")
    }


    private fun generateExpectedSignatures(body: String, nodes: List<Node>, contentType:String): Signatures {
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

    private fun toRegisterResponse(node: Node): RegisterResponse {
        return RegisterResponse(
            node.host,
            node.port,
            timeout,
            xGameTimestamp
        )
    }


    @PreDestroy
    fun onDestroy(){
        if (registerPort == -1) return // Coordinator just dies
        val unregisterUrl = "http://$registerHost:$registerPort/unregister-node"
        val unregisterParams = "?uuid=$UUID&salt=${this.salt}"
        val url = unregisterUrl + unregisterParams
        try {
            restTemplate.postForEntity<String>(url)
        } catch (e: RestClientException){
            print("Could not unregister to: $url")
            throw e
        }
    }
}
