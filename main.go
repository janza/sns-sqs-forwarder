package main

import (
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Publisher interface {
	Publish(msg string) error
}

func Publish(p Publisher, msg string) error {
	return p.Publish(msg)
}

type Subscription struct {
	Type  string
	Topic string
}

type SqsSubscription struct {
	*Subscription
	QueueName string
	Endpoint  string
}

type Config struct {
	Subscriptions []SqsSubscription
	Port          string
}

func (s SqsSubscription) Publish(id string, msg string) error {
	snsMessage := map[string]string{
		"Type":      "Notification",
		"MessageId": id,
		"Message":   msg,
		"Timestamp": time.Now().UTC().Format(time.RFC3339),
		"TopicArn":  s.Topic,
	}

	snsMessageJson, err := json.Marshal(snsMessage)

	if err != nil {
		return err
	}

	messageBody := string(snsMessageJson)

	fmt.Printf("Dispatching to: [%s] -> %s\n", s.QueueName, messageBody)

	resp, err := http.PostForm(
		fmt.Sprintf("%s?QueueName=%s", s.Endpoint, s.QueueName),
		url.Values{
			"Action":      {"SendMessage"},
			"Version":     {"2012-11-05"},
			"QueueUrl":    {s.QueueName},
			"MessageBody": {messageBody},
		})
	resp.Body.Close()
	return err
}

type SnsReply struct {
	XMLName   xml.Name `xml:"PublishResponse"`
	Namespace string   `xml:"xmlns,attr"`
	MessageId string   `xml:"PublishResult>MessageId"`
	RequestId string   `xml:"ResponseMetadata>ResponseMetadata"`
}

func pseudo_uuid() (uuid string) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return
	}
    uuid = strings.ToLower(
        fmt.Sprintf("%X-%X-%X-%X-%X",
        b[0:4], b[4:6], b[6:8], b[8:10], b[10:]))
	return
}

func main() {
    configPath := flag.String("config", "./config.json", "path to config file")
    flag.Parse()

	file, err := ioutil.ReadFile(*configPath)

	if err != nil {
        fmt.Printf("Error reading config: %s", err)
		return
	}

	var config Config
	err = json.Unmarshal(file, &config)

	if err != nil {
		fmt.Printf("Error parsing config.json: %s", err)
		return
	}

	for _, s := range config.Subscriptions {
		fmt.Printf("New subscription: [%s] -> %s\n", s.Topic, s.QueueName)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
            http.Error(w, "Something went wrong reading request body", 500)
            return
		}

		values, err := url.ParseQuery(string(body))

        if err != nil {
            http.Error(w, "Can't parse request body", 400)
            return
        }

		if values["Action"][0] != "Publish" {
            http.Error(w, "I can only handle Publish action", 400)
			return
		}
		messageId := pseudo_uuid()

		for _, s := range config.Subscriptions {
			if s.Topic == values["TopicArn"][0] {
				s.Publish(messageId, values["Message"][0])
			}
		}

		reply, _ := xml.Marshal(&SnsReply{
			MessageId: messageId,
			RequestId: pseudo_uuid(),
			Namespace: "http://sns.amazonaws.com/doc/2010-03-31/",
		})

		fmt.Fprintf(w, "%s", string(reply))
	})

	http.ListenAndServe(":"+config.Port, nil)
}
