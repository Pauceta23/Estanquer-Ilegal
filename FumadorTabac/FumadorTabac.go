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

func demanarTabac(ch *amqp.Channel, nom string) {
	body := "tabac"
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
	fmt.Print("Sóm fumador. Tinc mistos però me falta tabac\n")
	conn := conectarServidor()
	defer conn.Close()
	ch := obrirCanal(conn)
	defer ch.Close()
	q1 := declararCuaTabac(ch)
	q2 := declararCuaPeticions(ch)
	msgs := enregistrarConsumidor(ch, q1.Name)

	demanarTabac(ch, q2.Name)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		count := 0
		for d := range msgs {
			if bytes.Equal(d.Body, []byte("policia")) {
				break
			}
			fmt.Printf("He agafat el %s. Gràcies!\n", d.Body)
			for i := 0; i < 3; i++ {
				fmt.Print(". ")
				time.Sleep(500 * time.Millisecond)
			}
			fmt.Print("\nMe dones més tabac?\n")
			demanarTabac(ch, q2.Name)
			count++
		}
		wg.Done()
	}()

	wg.Wait()
	fmt.Print("Anem que ve la policia!\n")
}
