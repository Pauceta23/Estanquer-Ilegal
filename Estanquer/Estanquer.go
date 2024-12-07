package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

func conectarServidor() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	checkError(err)
	return conn
}

func obrirCanal(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	checkError(err)
	return ch
}

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
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()
	return q
}

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
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()
	return q
}

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
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()
	return q
}

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
	var wg sync.WaitGroup
	wg.Add(1)
	fmt.Print("Hola, som l'estanquer il·legal\n\n")

	conn := conectarServidor()
	defer conn.Close()
	ch := obrirCanal(conn)
	defer ch.Close()
	q := declararCuaPeticions(ch)
	q1 := declararCuaTabac(ch)
	q2 := declararCuaMistos(ch)
	msgs := peticions(ch, q.Name)

	go func() {
		ntabacs, nmistos := 0, 0
		for d := range msgs {
			if bytes.Equal(d.Body, []byte("policia")) {
				break
			}
			//Si piden tabaco, si no si piden mistos:
			if bytes.Equal(d.Body, []byte("tabac")) {
				ntabacs++
				publicarTabac(ch, q1.Name, ntabacs)
				fmt.Println("He posat el tabac", ntabacs, "damunt la taula")

			} else if bytes.Equal(d.Body, []byte("misto")) {
				nmistos++
				publicarMistos(ch, q2.Name, nmistos)
				fmt.Println("He posat el misto", nmistos, "damunt la taula")

			}
		}
		wg.Done()
	}()
	wg.Wait()
	fmt.Print("Uyuyuy la policia! Men vaig\n")
	for i := 0; i < 3; i++ {
		fmt.Print(". ")
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Print("Men duc la taula!!!!\n")
}
