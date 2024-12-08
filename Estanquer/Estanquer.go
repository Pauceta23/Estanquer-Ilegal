package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Funció bàsica per detectar errors
func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

// Funció bàsica per conectarse al servidor de rabbitmq
func conectarServidor() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	checkError(err)
	return conn
}

// Funció per obrir un canal al servidor de rabbitmq
func obrirCanal(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	checkError(err)
	return ch
}

// Funció per crear/unirse a la cua on els fumadors demanen tabac o mistos
func declararCuaPeticions(ch *amqp.Channel) amqp.Queue {
	q, err := ch.QueueDeclare(
		"Peticions", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	checkError(err)
	return q
}

// Funció per crear/unirse a la cua on l'estanquer deixa el tabac
func declararCuaTabac(ch *amqp.Channel) amqp.Queue {
	q, err := ch.QueueDeclare(
		"taulaTabac", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	checkError(err)
	return q
}

// Funció per crear/unirse a la cua on l'estanquer deixa els mistos
func declararCuaMistos(ch *amqp.Channel) amqp.Queue {
	q, err := ch.QueueDeclare(
		"taulaMistos", // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	checkError(err)
	return q
}

// Funció per publicar un n tabac a la cua de tabacs
func publicarTabac(ch *amqp.Channel, nom string, ntabacs int) {
	body := fmt.Sprintf("tabac %d", ntabacs)
	err := ch.Publish(
		"",    // exchange
		nom,   // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			CorrelationId: "estanquer",
			ContentType:   "text/plain",
			Body:          []byte(body),
		})
	checkError(err)
}

// Funció per publicar un n misto a la cua de mistos
func publicarMistos(ch *amqp.Channel, nom string, nmistos int) {
	body := fmt.Sprintf("misto %d", nmistos)
	err := ch.Publish(
		"",    // exchange
		nom,   // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	checkError(err)
}

// Funció per rebre les peticions que fan els fumadors
func peticions(ch *amqp.Channel, nom string) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		nom,   // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	checkError(err)
	return msgs
}

func main() {
	fmt.Print("Hola, som l'estanquer il·legal\n\n")

	var wg sync.WaitGroup //Variable per sincronizar el fil main i el fil fill
	wg.Add(1)             //Afegim el fil que s'executarà

	//Feim tota la gestió de rabbitmq
	conn := conectarServidor()
	defer conn.Close()
	ch := obrirCanal(conn)
	defer ch.Close()
	qP := declararCuaPeticions(ch)
	qT := declararCuaTabac(ch)
	qM := declararCuaMistos(ch)
	msgs := peticions(ch, qP.Name)

	go func() {
		ntabacs, nmistos := 0, 0 //Comptador de tabac i mistos per identificar-los
		for d := range msgs {    //Per cada missatge
			if bytes.Equal(d.Body, []byte("policia")) { //Si és la policia, sortim
				fmt.Print("Uyuyuy la policia! Men vaig\n")
				break
			}
			//Si demanen tabac, si no si demanen mistos:
			if bytes.Equal(d.Body, []byte("tabac")) {
				ntabacs++
				publicarTabac(ch, qT.Name, ntabacs)
				fmt.Println("He posat el tabac", ntabacs, "damunt la taula")

			} else if bytes.Equal(d.Body, []byte("misto")) {
				nmistos++
				publicarMistos(ch, qM.Name, nmistos)
				fmt.Println("He posat el misto", nmistos, "damunt la taula")

			}
		}
		wg.Done() //Avisam que ja hem acabat
	}()
	wg.Wait() //Espera fins que no hagui acabat la rutina de go

	//Simulació de replegació de les taules (cues)
	for i := 0; i < 3; i++ {
		fmt.Print(". ")
		time.Sleep(time.Second)
	}
	//Esborrament de les cues com es demana
	ch.QueueDelete(qP.Name, false, false, false)
	ch.QueueDelete(qT.Name, false, false, false)
	ch.QueueDelete(qM.Name, false, false, false)
	fmt.Print("Men duc la taula!!!!\n")
}
