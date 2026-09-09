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
	"encoding/binary"
	// "encoding/hex"
	"encoding/base64"

	"github.com/supabase-community/supabase-go"

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
	UserId          string      `json:"userId"`
	ApplicationId   string      `json:"applicationId"`
	DeviceType      string      `json:"deviceType"`
	DevAddr         string      `json:"devAddr"`
	DevEUI          string      `json:"devEUI"`
	Name            string      `json:"name"`
	Payload         interface{} `json:"payload"`
}

type DecodedSensorData struct {
	Timestamp      int64   `json:"timestamp"`
	AirTemperature float64 `json:"air_temperature"`
	AirHumidity    float64 `json:"air_humidity"`
	SoilMoisture   float64 `json:"soil_moisture"`
	Luminosity     float64 `json:"luminosity"`
	Battery        float64 `json:"battery"`
	Validity       bool    `json:"validity"`
}


// Struct limpa e focada no JSON de Aplicação do ChirpStack v4
type ChirpStackPackage struct {
	DeviceInfo struct {
		DeviceName string `json:"deviceName"`
		DevEUI     string `json:"devEUI"`
	} `json:"deviceInfo"`
	DevAddr string `json:"devAddr"`
	Data    string `json:"data"` // Aqui vem o Base64: "AAABggn2F3ARlB9AJxAB"
}

// Decodifica a string Base64 e aplica os fatores de escala
func decodeChirpStackPayload(base64Str string) (*DecodedSensorData, error) {
	// Converte de Base64 para array de bytes
	data, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, err
	}

	if len(data) < 15 {
		return nil, fmt.Errorf("payload incompleto: esperado 15 bytes, recebido %d", len(data))
	}

	// Extração usando leitura Big-Endian
	timestampRaw := binary.BigEndian.Uint32(data[0:4])
	airTempRaw := int16(binary.BigEndian.Uint16(data[4:6]))
	airHumRaw := binary.BigEndian.Uint16(data[6:8])
	soilMoistRaw := binary.BigEndian.Uint16(data[8:10])
	lumRaw := binary.BigEndian.Uint16(data[10:12])
	batteryRaw := binary.BigEndian.Uint16(data[12:14])
	validityRaw := data[14] == 1

	// Montagem da estrutura
	return &DecodedSensorData{
		Timestamp:      int64(timestampRaw),
		AirTemperature: float64(airTempRaw) / 100.0,
		AirHumidity:    float64(airHumRaw) / 100.0,
		SoilMoisture:   float64(soilMoistRaw) / 100.0,
		Luminosity:     float64(lumRaw) / 100.0,
		Battery:        float64(batteryRaw) / 100.0,
		Validity:       validityRaw,
	}, nil
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

func (eb *EventBus) publishToBus(payload []byte, userId string, deviceType string, devEUI string) {
	log.Printf("[EVENT BUS] Publicando para a Exchange: %s.%s.%s", deviceType, userId, devEUI)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // O padrão de roteamento que o Cache-Service espera (device.#)
    // Exemplo: device.fazenda2.7894567
    routingKey := fmt.Sprintf("%s.%s.%s", deviceType, userId, devEUI)

    // Publicação para o Ecossistema (Cache e outros que usem a exchange)
    err := eb.channel.PublishWithContext(ctx,
        "telemetria_exchange", // Exchange
        routingKey,            // Etiqueta: device.userId.devEUI
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

// passe o pool de conexão (db *sql.DB) como argumento. Isso salva muita memória e CPU!
func findUserId(client *supabase.Client, devEUI string, devAddr string) (string, error) {
	// Estrutura para receber o JSON de resposta da API
	var results []struct {
		UserId string `json:"userId"`
	}

	// Fazendo a requisição na API:
	// SELECT userId FROM smartfarm_users WHERE devices CONTAINS {devEUI}
	filterValue := fmt.Sprintf("{\"%s\":\"%s\"}", devEUI, devAddr) // Formato exigido para arrays no Postgres/PostgREST
	
	// Executa a query via API
	_, err := client.From("users").
		Select("userId", "exact", false).
		Filter("devices", "cs", filterValue). // 'cs' significa 'contains'
		ExecuteTo(&results)

	if err != nil {
		return "", fmt.Errorf("erro na requisição à API do Supabase: %v", err)
	}

	// Se o array voltar vazio, nenhum usuário é dono desse dispositivo
	if len(results) == 0 {
		return "", fmt.Errorf("nenhum produtor encontrado com o sensor: %s", devEUI)
	}

	// Retorna o UserId do primeiro usuário encontrado
	return results[0].UserId, nil
}


func main() {
	// Configura o Barramento de Eventos
	bus := setupEventBus()
	defer bus.conn.Close()
	defer bus.channel.Close()

	// Configura o MQTT
	opts := mqtt.NewClientOptions()
	// opts.AddBroker("tcp://localhost:1883")
	// opts.AddBroker("tcp://mqtt-broker:1883")
	opts.AddBroker("tcp://networkserver2.maua.br:1883")
	opts.SetClientID("public_listener_" + fmt.Sprint(time.Now().Unix())) // se não pode dar erro de clientId duplicado (só uso isso no networkserver)
	// opts.SetClientID("public")
	opts.SetUsername("public")
	opts.SetPassword("public")
	opts.SetCleanSession(false)

	// supa / ou qlqr bdd
	fmt.Println("Iniciando cliente do Supabase")
	supabaseURL := os.Getenv("SUPABASE_URL")
	supabaseKey := os.Getenv("SUPABASE_KEY")
	
	// se as variáveis estiverem vazias, o log avisa
    if supabaseURL == "" || supabaseKey == "" {
        log.Fatal("ERRO: SUPABASE_URL ou SUPABASE_KEY não configuradas nas variáveis de ambiente!")
    }
    
    db, erro := supabase.NewClient(supabaseURL, supabaseKey, nil)
    if erro != nil {
        fmt.Printf("falha ao inicializar o cliente do Supabase: %v", erro)
    }
    fmt.Println("Cliente Supabase inicializado com sucesso!")


	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("Ingestor conectado ao Broker MQTT")

		// c.Subscribe("application/c914c505-9a26-4c4b-acf1-400fa51514a0/device/5e76ce4fd99eefe3/event/up", 1, func(client mqtt.Client, msg mqtt.Message) {
		// c.Subscribe("application/c914c505-9a26-4c4b-acf1-400fa51514a0/device/+/event/up", 1, func(client mqtt.Client, msg mqtt.Message) {
		c.Subscribe("application/c914c505-9a26-4c4b-acf1-400fa51514a0/device/+/event/up", 1, func(client mqtt.Client, msg mqtt.Message) {
		// c.Subscribe("application/+/device/+/event/up", 1, func(client mqtt.Client, msg mqtt.Message) {

			fmt.Printf("\nMQTT Recebido: %s\nTópico: %s\n\n", string(msg.Payload()), msg.Topic())
			topic := msg.Topic()
        	parts := strings.Split(topic, "/")
        
			if len(parts) >= 4 {
				applicationId := parts[1] // Application tá no tópico (nome da fazenda/ id da fazenda/ do usuario)
				deviceType := parts[2] // tipo do dispositivo (device)
				devEUI := parts[3] // id do dispositivo (device01, device02, etc)
			

				// Faz o parse do envelope estruturado do ChirpStack
				var envelope ChirpStackPackage
				if err := json.Unmarshal(msg.Payload(), &envelope); err != nil {
					log.Printf("Erro ao decodificar JSON bruto: %v", err)
					return
				}

				if envelope.Data == "" {
					log.Printf("Pacote ignorado: campo 'data' vazio.")
					return
				}

				// Como o novo JSON não tem 'deviceName' na raiz, usamos o DevAddr ou DevEUI como fallback de nome
				devAddr := envelope.DevAddr
				if devAddr == "" {
					devAddr = devEUI
				}
				deviceName := envelope.DeviceInfo.DeviceName
				if deviceName == "" {
					deviceName = devEUI
				}

				userId, err := findUserId(db,devEUI, devAddr)
				if err != nil {
					log.Printf("Erro ao buscar usuário para DevEUI (%s): %v", devEUI, err)
					userId = applicationId // fallback para evitar falha completa
				}

				// Executa a decodificação da string Hexadecimal
				decodedData, err := decodeChirpStackPayload(envelope.Data)
				if err != nil {
					log.Printf("Erro ao decodificar hexadecimal (%s): %v", envelope.Data, err)
					return
				}

				fmt.Printf("\n[MQTT SUCESSO] Recebido de DevEUI: %s", devEUI)
				fmt.Printf("\n[MQTT] Processando Sensor(%s): %s", devAddr, applicationId)

				custom := CustomPayload{
					UserId: userId, // Aqui o UserId é o nome da fazenda (proveniente do postgrees)
					ApplicationId: applicationId,
					DeviceType: deviceType,
					DevEUI: devEUI,
					DevAddr: devAddr,
					Name: deviceName, // Nome do dispositivo (pode ser o DevEUI caso nao venha na msg)
					Payload: decodedData, // Injeto o objeto ja estruturadao e mastigado para a sequencia
				}

				jsonPayload, err := json.Marshal(custom)
				if err != nil {
					log.Printf("Erro ao serializar payload: %v", err)
					return
				}

				fmt.Printf("\n[CHIRPSTACK PARSER] deviceName: %s | devEUI: %s | devAddr: %s | Temp: %.2f°C | Hum: %.2f%% | Soil: %.2f%% | Lum: %.2f%% | Bat: %.2f%% | Valid: %t\n", deviceName, devEUI, devAddr, decodedData.AirTemperature, decodedData.AirHumidity, decodedData.SoilMoisture, decodedData.Luminosity, decodedData.Battery, decodedData.Validity)

				// Despacha o JSON completamente mastigado para o RabbitMQ
				bus.publishToBus(jsonPayload, userId, deviceType, devEUI)
				fmt.Printf("\n\n\nCustomPayload: %s\n FIM", string(jsonPayload))
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



// * Formato e codificação da Payload (em ordem):
//     - Timestamp ---------> 4 bytes - uint32_t (Big-Endian)
//     - Temperatura do Ar -> 2 bytes - int16_t * 100 (suporta negativos)
//     - Umidade do Ar -----> 2 bytes - uint16_t * 100
//     - Umidade do Solo ---> 2 bytes - uint16_t * 100
//     - Luminosidade ------> 2 bytes - uint16_t * 100
//     - Bateria -----------> 2 bytes - uint16_t * 100
//     - Validade ----------> 1 byte - bool

//     === Exemplo: 
//         - frm_payload = 00000087 09f6 1770 1194 1f40 2710 01
//         - Timestamp ---------> 00 00 00 87 -> (int dec) 135
//         - Temperatura do Ar -> 09 f6 -------> (.2f dec) 2550 -- /100 --> 25.50 
//         - Umidade do Ar -----> 17 70 -------> (.2f dec) 6000 -- /100 --> 60.00
//         - Umidade do Solo ---> 11 94 -------> (.2f dec) 4500 -- /100 --> 45.00
//         - Luminosidade ------> 1f 40 -------> (.2f dec) 8000 -- /100 --> 80.00
//         - Bateria -----------> 27 10 -------> (.2f dec) 10000 - /100 --> 100.00
//         - Validade ----------> 01 ----------> (int dec) 1