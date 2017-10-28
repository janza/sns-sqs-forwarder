package main

import (
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
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

type SqsSubscription struct {
	*Subscription
	QueueName string
	Endpoint  string
	Raw       bool
}

func (s SqsSubscription) Publish(id string, msg string) error {
	var messageBody string
	if s.Raw {
		messageBody = msg
	} else {
		snsMessage := map[string]string{
			"Type":      "Notification",
			"MessageId": id,
			"Message":   msg,
			"Timestamp": time.Now().UTC().Format(time.RFC3339),
			"TopicArn":  s.Topic,
		}

		snsMessageJSON, err := json.Marshal(snsMessage)
		if err != nil {
			return err
		}
		messageBody = string(snsMessageJSON)
	}

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
	MessageID string   `xml:"PublishResult>MessageId"`
	RequestID string   `xml:"ResponseMetadata>ResponseMetadata"`
}

func pseudoUUID() (uuid string) {
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
	config, err := getConfig()

	if err != nil {
		fmt.Printf("Error parsing config.json: %s\n", err)
		return
	}

	for _, s := range config.Subscriptions {
		fmt.Printf("New subscription: [%s] -> %s\n", s.Topic, s.QueueName)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Something went wrong reading request body", 500)
			fmt.Printf("Something went wrong reading request body: %s\n", err)
			return
		}

		values, err := url.ParseQuery(string(body))

		if err != nil {
			http.Error(w, "Can't parse request body", 400)
			fmt.Printf("Can't parse request body: %s\n", err)
			return
		}

		if values["Action"][0] != "Publish" {
			http.Error(w, "I can only handle Publish action", 400)
			fmt.Printf("I can only handle Publish action: %s\n", err)
			return
		}

		messageID := pseudoUUID()

		for _, s := range config.Subscriptions {
			if s.Topic == values["TopicArn"][0] {
				s.Publish(messageID, values["Message"][0])
			}
		}

		reply, _ := xml.Marshal(&SnsReply{
			MessageID: messageID,
			RequestID: pseudoUUID(),
			Namespace: "http://sns.amazonaws.com/doc/2010-03-31/",
		})

		fmt.Fprintf(w, "%s", string(reply))
	})

	http.ListenAndServe(":"+config.Port, nil)
}
