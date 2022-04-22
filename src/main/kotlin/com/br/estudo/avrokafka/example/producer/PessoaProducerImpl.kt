package com.br.estudo.avrokafka.example.producer

import com.br.estudo.avrokafka.example.entity.Pessoa
import com.br.estudo.avrokafka.example.entity.PessoaDTO
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFuture
import org.springframework.util.concurrent.ListenableFutureCallback
import java.time.LocalDate

@Component
class PessoaProducerImpl(
    private val pessoaTemplate: KafkaTemplate<String, PessoaDTO>
) {

    val topicName = "Pessoa"

    fun persist(messageId: String, payload: Pessoa){
        val dto = createDTO(payload)
        sendPessoaMessage(messageId, dto)
    }

    private fun sendPessoaMessage(messageId: String, pessoaDTO: PessoaDTO) {
        val message = createMessageWithHeaders(messageId, pessoaDTO, topicName)

        val future: ListenableFuture<SendResult<String, PessoaDTO>> = pessoaTemplate.send(message) //envia kafka _> onSuccess

        future.addCallback(object : ListenableFutureCallback<SendResult<String, PessoaDTO>>{
            override fun onSuccess(result: SendResult<String, PessoaDTO>?){
                println("Evento pessoa enviado para o HUB, MessageID: $messageId")
            }
            override fun onFailure(ex: Throwable){
                //Deve entrar o New Relic (para a mensagem de cid 1234 ocorreu erro)
                println("Erro ao enviar evento pessoa enviado para o HUB, MessageID: $messageId")
            }
        })
    }

    private fun createMessageWithHeaders(messageId: String, pessoaDTO: PessoaDTO, topicName: String): Message<PessoaDTO> {
        return MessageBuilder.withPayload(pessoaDTO)
            .setHeader("hash", pessoaDTO.hashCode())
            .setHeader("version", "1.0.0")
            .setHeader("endOfLife", LocalDate.now().plusDays(1))
            .setHeader("type", "fct")
            .setHeader("cid", messageId)
            .setHeader(KafkaHeaders.TOPIC, topicName)
            .setHeader(KafkaHeaders.MESSAGE_KEY, messageId)
            .build()
    }

    private fun createDTO(payload: Pessoa): PessoaDTO {
        return PessoaDTO.newBuilder()
            .setNome(payload.nome)
            .setSobrenome(payload.sobrenome)
            .setIdade(payload.idade)
            .build()
    }

}