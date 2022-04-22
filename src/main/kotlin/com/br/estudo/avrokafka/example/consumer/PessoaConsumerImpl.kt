package com.br.estudo.avrokafka.example.consumer

import com.br.estudo.avrokafka.example.entity.Pessoa
import com.br.estudo.avrokafka.example.entity.PessoaDTO
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.PartitionOffset
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class PessoaConsumerImpl {

    @KafkaListener(topics = ["Pessoa"], groupId = "pessoa-consumer")
    /*@KafkaListener(
        id = "pessoa-consumer", topicPartitions = [TopicPartition(
            topic = "Pessoa",
            partitions = ["0"],
            partitionOffsets = arrayOf(PartitionOffset(partition = "*", initialOffset = "0"))
        )]
    )*/
    fun consumer(@Payload pessoaDTO: PessoaDTO) {
        val pessoa = Pessoa(pessoaDTO.getNome().toString(), pessoaDTO.getSobrenome().toString(), pessoaDTO.getIdade())
        println("Pessoa Recebida: ${pessoa.toString()}")
    }

}