package main

import (
	"fmt"
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

// Cream el exchange de tipus fanout per fer un "broadcast"
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

// Unim cada cua al exchange per que el missatge broadcast arribi
func enganxarCuesExchange(ch *amqp.Channel, noms [3]string) {
	for _, nom := range noms {
		err := ch.QueueBind(
			nom,      // queue name
			"",       // routing key
			"avisar", // exchange
			false,
			nil,
		)
		checkError(err)
	}
}

// Funció per enviar el missatge broadcast de la policia al exchange
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
	//Gestió de rabbitmq
	conn := conectarServidor()
	defer conn.Close()
	ch := obrirCanal(conn)
	defer ch.Close()
	qP := declararCuaPeticions(ch) //Declarar les cues no és necessari
	qT := declararCuaTabac(ch)     //però és per evitar errors si es vol
	qM := declararCuaMistos(ch)    //executar el Delator el primer de tots
	crearExchange(ch)
	enganxarCuesExchange(ch, [3]string{qP.Name, qT.Name, qM.Name})

	//Feim una espera llarga i després avisam a la policia
	time.Sleep(10 * time.Second)
	fmt.Print("No sóm fumador. ALERTA! Que ve la policia!\n\n")
	avisarPolicia(ch)
	for i := 0; i < 3; i++ {
		fmt.Print(". ")
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Print("\n")
}
