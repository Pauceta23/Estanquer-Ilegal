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

func enregistrarConsumidor(ch *amqp.Channel, nom string) <-chan amqp.Delivery {
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

func demanarMistos(ch *amqp.Channel, nom string) {
	body := "misto"
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

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	fmt.Print("Sóm fumador. Tinc tabac però me falten mistos\n")
	conn := conectarServidor()
	defer conn.Close()
	ch := obrirCanal(conn)
	defer ch.Close()
	q1 := declararCuaMistos(ch)
	q2 := declararCuaPeticions(ch)
	msgs := enregistrarConsumidor(ch, q1.Name)

	demanarMistos(ch, q2.Name)
	go func() {

		for d := range msgs {
			if bytes.Equal(d.Body, []byte("policia")) {
				break
			}
			//if bytes.Equal(d.Body, []byte("tabac 3")) {
			fmt.Printf("He agafat el %s. Gràcies!\n", d.Body)
			for i := 0; i < 3; i++ {
				fmt.Print(". ")
				time.Sleep(500 * time.Millisecond)
			}
			fmt.Print("\nMe dones un altre misto?\n")
			demanarMistos(ch, q2.Name)
		}
		wg.Done()
	}()

	wg.Wait()
	fmt.Print("Anem que ve la policia!\n")
}
