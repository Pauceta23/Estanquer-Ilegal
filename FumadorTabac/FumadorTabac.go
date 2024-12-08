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

// Funció per crear/unirse a la cua on es recollirà el tabac
func enregistrarConsumidor(ch *amqp.Channel, nom string) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		nom,   // queue
		"",    // consumer
		false, // auto-ack		Veure que auto-ack està a fals per a que
		false, // exclusive		no s'esborri el missatge de policia i la
		false, // no-local		resta de fumadors el puguin veure
		false, // no-wait
		nil,   // args
	)
	checkError(err)
	return msgs
}

// Funció per demanar un tabac
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

	var wg sync.WaitGroup //Variable per sincronizar el fil main i el fil fill
	wg.Add(1)             //Afegim el fil que s'executarà

	//Gestió del rabbitmq
	conn := conectarServidor()
	defer conn.Close()
	ch := obrirCanal(conn)
	defer ch.Close()
	q1 := declararCuaTabac(ch)
	q2 := declararCuaPeticions(ch)
	msgs := enregistrarConsumidor(ch, q1.Name)

	demanarTabac(ch, q2.Name)

	go func() {
		demanarTabac(ch, q2.Name) //Feim la primera petició
		for d := range msgs {
			if bytes.Equal(d.Body, []byte("policia")) {
				break //Si es la policia sortim
			}
			//Si no es policia, es un tabac
			d.Ack(false) //Feim el ack per esborrar el missatge de la cua
			fmt.Printf("He agafat el %s. Gràcies!\n", d.Body)
			for i := 0; i < 3; i++ { //Espera
				fmt.Print(". ")
				time.Sleep(500 * time.Millisecond)
			}
			//Demanar un altre tabac i repetir procés
			fmt.Print("\nMe dones més tabac?\n")
			demanarTabac(ch, q2.Name)
		}
		wg.Done() //Avisam que ja hem acabat
	}()

	wg.Wait() //Esperam i una vegada terminat ens anam
	fmt.Print("Anem que ve la policia!\n")
}
