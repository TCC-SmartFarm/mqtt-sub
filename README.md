# MQTT sub (go)
## Descrição
O mqtt-ingestor é um microserviço desenvolvido em Go responsável por realizar a ponte entre a camada de borda (Sensores/Broker) e a camada de processamento de eventos. Ele atua como um Subscriber persistente que monitora o tráfego de telemetria e encaminha os dados para o barramento de mensagens (RabbitMQ), garantindo que nenhuma informação vinda do campo seja perdida.

## Bibliotecas Utilizadas e Justificativa
### paho.mqtt.golang:

Justificativa: É a biblioteca padrão de fato para implementações MQTT em Go. Ela oferece suporte completo a QoS 1 e 2, essencial para a meta de resiliência da infraestrutura (ODS 9), garantindo que as mensagens de sensores LoRa sejam entregues de forma confiável mesmo em condições de rede instáveis.

### streadway/amqp (Preparado):

Justificativa: Biblioteca para integração com o protocolo AMQP do RabbitMQ. Foi selecionada para permitir o desacoplamento entre a recepção do dado e o seu processamento final, permitindo que a plataforma escale horizontalmente conforme o número de fazendas monitoradas aumente.

## Arquitetura do Sub
O projeto utiliza um padrão de Event Hooks, onde a lógica de recepção de mensagens é separada da lógica de despacho.

### 1. Camada de Ingestão (MQTT)
O serviço se conecta ao Broker utilizando credenciais de segurança e uma Sessão Persistente (SetCleanSession(false)). Isso significa que, caso o ingestor fique offline para manutenção, o Broker armazenará as mensagens do campo e as entregará assim que o serviço retornar, cumprindo os requisitos de disponibilidade do projeto.

### 2. O Barramento de Eventos (RabbitMQ)
A ideia central deste módulo é não processar o dado diretamente. Em vez de salvar no banco de dados imediatamente, o Ingestor "empurra" a mensagem para uma fila no RabbitMQ.

#### Por que o Barramento? 
Isso evita gargalos no banco de dados MongoDB durante picos de transmissão e permite que múltiplos serviços (como o sistema de alertas e o módulo de séries temporais) consumam o mesmo dado simultaneamente.