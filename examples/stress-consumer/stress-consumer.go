// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/log"

	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
)

const (
	MaxRetryCount = 100000
)

var lr = logrus.StandardLogger()

// keep message stats
var msgReceived = uint64(0)
var bytesReceived = uint64(0)

func CreateSubsc(client pulsar.Client, appId uint64, tenant string, namespace string, subname string, topicname string) (pulsar.Consumer, error) {
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       "persistent://" + tenant + "/" + namespace + "/" + topicname,
		Topics:                      nil,
		TopicsPattern:               "",
		AutoDiscoveryPeriod:         0,
		SubscriptionName:            strconv.FormatUint(appId, 10) + "_subsc_" + subname + "_consumer",
		Properties:                  nil,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		// DLQ:                         &dlqPolicy,
		KeySharedPolicy:            nil,
		RetryEnable:                false, // todo
		MessageChannel:             nil,
		ReceiverQueueSize:          1000,
		NackRedeliveryDelay:        time.Second,
		ReadCompacted:              false,
		ReplicateSubscriptionState: false,
		Interceptors:               nil,
		Schema:                     nil,
	})
	if err != nil {
		lr.Warnf("create subscribe error %v", err)
		return nil, err
	}
	return consumer, nil
}

func stopCh() <-chan struct{} {
	stop := make(chan struct{})
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		close(stop)
	}()
	return stop
}

// AddUint64 atomically adds delta to *addr and returns the new value.
// To subtract a signed positive constant value c from x, do AddUint64(&x, ^uint64(c-1)).
// In particular, to decrement x, do AddUint64(&x, ^uint64(0)).
// func AddUint64(addr *uint64, delta uint64) (new uint64)
func consumerMsg(consumer pulsar.Consumer, stop <-chan struct{}) {
	for {
		ctxTimeout, _ := context.WithTimeout(context.Background(), 1*time.Second) // wait one second if no message
		msg, err := consumer.Receive(ctxTimeout)
		select {
		case <-ctxTimeout.Done(): // time out
			// lr.Errorf("consumer recv timeout", consumer)
			time.Sleep(5 * time.Millisecond)
		default:
			if err != nil {
				lr.Errorf("consumer recv fail %v, err %v", consumer, err)
				break
			}
			atomic.AddUint64(&msgReceived, 1)
			atomic.AddUint64(&bytesReceived, uint64(len(msg.Payload())))
			consumer.Ack(msg)
		case <-stop:
			lr.Errorf("consumer stop %v", consumer)
			return
		}
	}
}

func main() {

	var (
		tenant        string
		namespace     string
		topicname     string
		appid         int64
		subcnt        int
		cluster_token string
		cluster_url   string
	)

	//---Define the various flags---
	flag.StringVar(&cluster_token, "token", "", "token for the cluster")
	flag.StringVar(&cluster_url, "url", "", "url for the cluster")
	flag.StringVar(&tenant, "tenant", "", "tenant for the topic")
	flag.StringVar(&namespace, "namespace", "", "namespace for the topic")
	flag.StringVar(&topicname, "topic", "", "name for the topic")
	flag.Int64Var(&appid, "appid", 0, "appid for the tenant")
	flag.IntVar(&subcnt, "subcnt", 10, "sub cnt for the topic")
	//---parse the command line into the defined flags---
	flag.Parse()

	fmt.Println("cluster_token:", cluster_token)
	fmt.Println("cluster_url:", cluster_url)
	fmt.Println("tenant:", tenant)
	fmt.Println("namespace:", namespace)
	fmt.Println("topicname:", topicname)
	fmt.Println("appid:", appid)

	level, err := logrus.ParseLevel("info")
	if err != nil {
		level = logrus.InfoLevel
	}

	lr.SetLevel(level)
	lr.SetOutput(&lumberjack.Logger{
		Filename:   "demo.log",
		MaxSize:    500 * 1024 * 1024, // megabytes
		MaxBackups: 10,
	})

	auth := pulsar.NewAuthenticationToken(cluster_token)
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            cluster_url,
		Logger:         log.NewLoggerWithLogrus(lr),
		Authentication: auth,
	})
	if err != nil {
		lr.Fatalf("create pulsar client error %v", err)
	}

	var subname string

	stop := stopCh()
	for i := 0; i < subcnt; i++ {
		subname = "sub" + strconv.FormatUint((uint64(i)), 10)
		s := tenant + "/" + namespace + "/" + strconv.FormatUint((uint64(appid)), 10) + "_subsc_" + subname
		lr.Infof("s %s", s)
		Topic := "persistent://" + tenant + "/" + namespace + "/" + topicname
		lr.Infof("Topic %s", Topic)
		SubscriptionName := strconv.FormatUint((uint64(appid)), 10) + "_subsc_" + subname + "_consumer"
		lr.Infof("SubscriptionName %s", SubscriptionName)

		consumer, err := CreateSubsc(client, uint64(appid), tenant, namespace, subname, topicname)
		if err != nil {
			lr.Errorf("create subs fail error %v, consumer %v", err, consumer)
		}
		go consumerMsg(consumer, stop)
	}

	defer client.Close()

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			currentMsgReceived := atomic.SwapUint64(&msgReceived, 0)
			currentBytesReceived := atomic.SwapUint64(&bytesReceived, 0)
			msgRate := float64(currentMsgReceived) / float64(10)
			bytesRate := float64(currentBytesReceived) / float64(10)
			lr.Infof(`Stats - Consume rate: %6.1f msg/s - %6.1f Mbps`, msgRate, bytesRate*8/1024/1024)
		case <-stop:
			lr.Error("stop stop stop")
			return
		}
	}
}
