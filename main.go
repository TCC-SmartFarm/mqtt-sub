package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"context"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Estrutura para gerenciar a conexão com o RabbitMQ
type EventBus struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// Inicializa a conexão com o barramento de eventos
func setupEventBus() *EventBus {
	// Em produção (Docker), use: amqp://admin:admin@barramento-de-eventos:5672/
	// conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	conn, err := amqp.Dial("amqp://admin:admin@barramento-de-eventos:5672/")

	if err != nil {
		log.Fatalf("Falha ao conectar no RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Falha ao abrir canal no RabbitMQ: %v", err)
	}

	// Declara a fila onde os dados dos sensores serão acumulados
	q, err := ch.QueueDeclare(
		"fila_cache", // Nome da fila
		true,                // Durável (sobrevive ao restart do RabbitMQ)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Falha ao declarar fila: %v", err)
	}

	return &EventBus{conn: conn, channel: ch, queue: q}
}

func (eb *EventBus) publishToBus(payload []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := eb.channel.PublishWithContext(ctx,
		"",            // exchange
		eb.queue.Name, // routing key (nome da fila)
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
			Timestamp:   time.Now(),
		})

	if err != nil {
		log.Printf("Erro ao publicar no barramento: %v", err)
	} else {
		fmt.Printf("[EVENT BUS] Mensagem enviada para a fila: %s\n", eb.queue.Name)
	}
}

func main() {
	// 1. Configura o Barramento de Eventos
	bus := setupEventBus()
	defer bus.conn.Close()
	defer bus.channel.Close()

	// 2. Configura o MQTT
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://mqtt-broker:1883")
	opts.SetClientID("mqtt-sub")
	opts.SetUsername("sensor")
	opts.SetPassword("123")
	opts.SetCleanSession(false)

	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("Ingestor conectado ao Broker MQTT")
		c.Subscribe("campo/+/sensor/+/dados", 1, func(client mqtt.Client, msg mqtt.Message) {
			fmt.Printf("\nMQTT Recebido: %s", string(msg.Payload()))
			
			// ENVIAR PARA O BARRAMENTO
			bus.publishToBus(msg.Payload())
		})
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// Mantém o processo vivo
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}