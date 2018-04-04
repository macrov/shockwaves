package main

import (
    "github.com/gorilla/websocket"
    "fmt"
    "compress/gzip"
    "bytes"
    "io/ioutil"
    "github.com/bitly/go-simplejson"
    "os"
    "github.com/spf13/viper"

    "gopkg.in/mgo.v2"
    "encoding/json"
)

type Message struct {
    Ch string
    Ts int64
    Tick Tick
}

type Tick struct {
    Id int64
    Ts int64
    Data []Trade
}

type Trade struct {
    Amount  float64
    Direction string
    Price float64
    Ts int64
}
var (
    controlChan chan string = make(chan string)
    session *mgo.Session
    db *mgo.Database

)

func GzipDecode(in []byte) ([]byte, error) {
    reader, err := gzip.NewReader(bytes.NewReader(in))
    if err != nil {
        var out []byte
        return out, err
    }
    defer reader.Close()

    return ioutil.ReadAll(reader)
}


func WorkThread(symbol string) {
    collection := db.C(symbol)
    saveFileName := fmt.Sprintf("trade_details_%s.txt", symbol)

    tradeDetailFile, err := os.OpenFile(saveFileName, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0644 )
    if err != nil {
        fmt.Println("file open failed : ", err)
        return
    }
    //bufferWriter := bufio.NewWriter(tradeDetailFile)
    defer tradeDetailFile.Close()

    c, _, err := websocket.DefaultDialer.Dial("ws://api.huobi.pro/ws", nil)
    if err != nil {
        fmt.Println("ws dail err : ", err)
        controlChan<-symbol
        return
    }

    message := []byte(fmt.Sprintf("{\"sub\":\"market.%s.trade.detail\"}", symbol))
    err = c.WriteMessage(websocket.TextMessage, message)
    if err != nil {
        fmt.Println("write err :", err)
        controlChan<-symbol
        return
    }

    for {
        _, msg, err := c.ReadMessage()
        if err != nil {
            fmt.Println("ws read error : ", err)
            controlChan<-symbol
            return
        }

        unzipMsg, err := GzipDecode(msg)
        if err != nil {
            fmt.Println("unzip error : ", err)
            controlChan<-symbol
            return
        }

        jsonResponse, err := simplejson.NewJson(unzipMsg)
        if err != nil {
            fmt.Errorf("json decode error : ", err)
            controlChan<-symbol
            return
        }

        fmt.Println("get messge : ", jsonResponse)

        if ping, err := jsonResponse.Get("ping").Int(); err == nil && ping != 0 {
            message = []byte(fmt.Sprintf("{\"pong\":%d}", ping))
            c.WriteMessage(websocket.TextMessage, message)
            fmt.Println("send pong messge : ", string(message))
            continue
        }


        m := Message{}
        if err := json.Unmarshal(unzipMsg, &m); err != nil {
            fmt.Println("unmarshal erro : ", err)
            return
        }
        
        for _,t := range m.Tick.Data {
            collection.Insert(t)
        }

    }
}

func main() {


    viper.SetConfigFile("./archaeologist.yaml")
    viper.AddConfigPath(".")

    err := viper.ReadInConfig()
    if err != nil {
        fmt.Println("read config err : ", err)
    }

    session, err = mgo.Dial("127.0.0.1:27017")

    if err != nil {
        fmt.Println("mongo dial err : ", err)
    }
    db = session.DB("trade_detail")

    symbols := viper.GetStringSlice("symbols")
    fmt.Println(symbols)

    for _, s := range symbols {
        go WorkThread(s)
    }

    for {
        symbol := <-controlChan
        go WorkThread(symbol)
    }
}
