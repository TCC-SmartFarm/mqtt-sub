package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"context"
	"strings"
	"encoding/json"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Estrutura para gerenciar a conexão com o RabbitMQ
type EventBus struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	// queue   amqp.Queue removi pois a definição vai ser dinamica
}

type CustomPayload struct {
	UserId     string      `json:"userId"`
	DeviceType string      `json:"deviceType"`
	DeviceId   string      `json:"deviceId"`
	Payload    interface{} `json:"payload"`
}

// Inicializa a conexão com o barramento de eventos // LEMBRAR DE MUDAR O NOME DO CONTAINER PARA CADA CASO DE DEPLOY!!!!
func setupEventBus() *EventBus {
	// Em produção (Docker), use: "amqp://admin:admin@barramento-de-eventos:5672/"    
	// "amqp://admin:admin@localhost:5672/"
	var conn *amqp.Connection
    var err error
    url := "amqp://admin:admin@barramento-de-eventos:5672/" 

    // Tenta conectar 5 vezes antes de desistir
    for i := 1; i <= 5; i++ {
        fmt.Printf("Tentando conectar ao RabbitMQ (Tentativa %d/5)...\n", i)
        conn, err = amqp.Dial(url)
        if err == nil {
            break
        }
        log.Printf("Aguardando RabbitMQ iniciar.%v", err)
        time.Sleep(5 * time.Second) // Espera 5 segundos para a próxima tentativa
    }

    if err != nil {
        log.Fatalf("Falha definitiva ao conectar no RabbitMQ: %v", err)
    }

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Falha ao abrir canal no RabbitMQ: %v", err)
    }

    fmt.Println("Conectado com sucesso ao RabbitMQ!")
    return &EventBus{conn: conn, channel: ch}
}

func (eb *EventBus) publishToBus(payload []byte, userId string, deviceType string, deviceId string) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // O padrão de roteamento que o Cache-Service espera (sensor.#)
    // Exemplo: sensor.fazenda2.7894567
    routingKey := fmt.Sprintf("%s.%s.%s", deviceType, userId, deviceId)	
	nomeDaFila := fmt.Sprintf("%s.%s.%s", deviceType, userId, deviceId)


	// fila específica para o dispositivo (opcional, mas pode ser útil para casos de uso específicos)
	//padrão de fila que eu usava antes
	_, err := eb.channel.QueueDeclare(
        nomeDaFila, // Agora usamos o nome passado por parâmetro
        true,       // Durável
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Printf("Erro ao declarar fila: %v", err)
        return
    }

	err = eb.channel.PublishWithContext(ctx,
		"",            // exchange
		nomeDaFila, // routing key (nome da fila)
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
			Timestamp:   time.Now(),
		})
	/////////////////////

    // Publicação para o Ecossistema (Cache e outros que usem a exchange)
    err = eb.channel.PublishWithContext(ctx,
        "telemetria_exchange", // Exchange
        routingKey,            // Etiqueta: sensor.userId.deviceId
        false,
        false,
        amqp.Publishing{
            ContentType: "application/json",
            Body:        payload,
            Timestamp:   time.Now(),
        })

    if err != nil {
        log.Printf("Erro ao publicar na telemetria_exchange: %v", err)
    }

    // Publicação para o Influx
    // Mas o ideal no TCC é o Influx também ser um Subscriber da Exchange!
    err = eb.channel.PublishWithContext(ctx,
        "",              // Default Exchange
        "fila_influx",   // Nome da fila direto
        false,
        false,
        amqp.Publishing{
            ContentType: "application/json",
            Body:        payload,
            Timestamp:   time.Now(),
        })

    if err != nil {
        log.Printf("Erro ao publicar para o Influx: %v", err)
    } else {
        fmt.Printf("[EVENT BUS] Mensagem roteada: %s\n", routingKey)
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
	opts.SetUsername("mqtt_sub")
	opts.SetPassword("mqtt_sub")
	opts.SetCleanSession(false)

	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("Ingestor conectado ao Broker MQTT")

		// topico onde apenas temos devices do tipo 'sensor'
		c.Subscribe("userId/+/sensor/+/dados", 1, func(client mqtt.Client, msg mqtt.Message) {
			fmt.Printf("\nMQTT Recebido: %s\nTópico: %s", string(msg.Payload()), msg.Topic())
			topic := msg.Topic()
        	parts := strings.Split(topic, "/")
        
			if len(parts) >= 4 {
				userId := parts[1] // userId tá no tópico (nome da fazenda/ id da fazenda/ do usuario)
				deviceType := parts[2] // tipo do dispositivo (sensor)
				deviceId := parts[3] // id do dispositivo (sensor01, sensor02, etc)
				
				fmt.Printf("\n[MQTT] Processando Sensor: %s", userId)

				custom := CustomPayload{
					UserId:    userId,
					DeviceType: deviceType,
					DeviceId: deviceId,
					Payload: json.RawMessage(msg.Payload()), // ou string(msg.Payload()) se não for JSON válido
				}

				jsonPayload, err := json.Marshal(custom)
				if err != nil {
					log.Printf("Erro ao serializar payload: %v", err)
					return
				}

				// Publica na fila específica do dispositivo
				bus.publishToBus(jsonPayload, userId, deviceType, deviceId)
			}
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