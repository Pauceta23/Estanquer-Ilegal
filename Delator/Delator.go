package main

import (
	"fmt"
	"os"

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

func crearExchange(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"avisar", // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	checkError(err)
}

func enganxarCuesExchange(ch *amqp.Channel, noms [3]string) {
	err := ch.QueueBind(
		noms[0],  // queue name
		"",       // routing key
		"avisar", // exchange
		false,
		nil,
	)
	checkError(err)
	err = ch.QueueBind(
		noms[1],  // queue name
		"",       // routing key
		"avisar", // exchange
		false,
		nil,
	)
	checkError(err)
	err = ch.QueueBind(
		noms[2],  // queue name
		"",       // routing key
		"avisar", // exchange
		false,
		nil,
	)
	checkError(err)
}

func avisarPolicia(ch *amqp.Channel) {
	body := "policia"
	err := ch.Publish(
		"avisar", // exchange
		"",       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	checkError(err)
}

func main() {
	conn := conectarServidor()
	defer conn.Close()
	ch := obrirCanal(conn)
	defer ch.Close()
	crearExchange(ch)
	enganxarCuesExchange(ch, [3]string{"Peticions", "taulaTabac", "taulaMistos"})

	//time.Sleep(10 * time.Second)
	fmt.Print("No sóm fumador. ALERTA! Que ve la policia!\n")
	avisarPolicia(ch)
	//desactivar todos los procesos
	os.Exit(0)
}
