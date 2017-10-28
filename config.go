package main

import (
	"flag"
	"io/ioutil"
	"encoding/json"
)

type Subscription struct {
	Type  string
	Topic string
}

type Config struct {
	Subscriptions []SqsSubscription
	Port          string
}

func getConfig() (Config, error) {
	configPath := flag.String("config", "./config.json", "path to config file")
	flag.Parse()

	file, err := ioutil.ReadFile(*configPath)

	if err != nil {
		return Config{}, err
	}

	var config Config
	err = json.Unmarshal(file, &config)

	if err != nil {
		return Config{}, nil
	}

	return config, nil
}