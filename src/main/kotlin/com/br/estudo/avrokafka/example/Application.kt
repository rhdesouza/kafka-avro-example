package com.br.estudo.avrokafka.example

import com.br.estudo.avrokafka.example.entity.Pessoa
import com.br.estudo.avrokafka.example.producer.PessoaProducerImpl
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class Application(
	val pessoaProducerImpl: PessoaProducerImpl
):ApplicationRunner{
	override fun run(args: ApplicationArguments?) {
		val pessoa = Pessoa("Rafael", "Henrique", 37)
		Thread.sleep(5000)
		pessoaProducerImpl.persist("10933679", pessoa)
	}
}

fun main(args: Array<String>) {
	runApplication<Application>(*args)
}
