package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

type requestError struct {
	Message string `json:"message,omitempty"`
}

type requestObject struct {
	City     string  `json:"city"`
	Date     int64   `json:"date"`
	Pressure float64 `json:"pressure,omitempty"`
	Humidity float64 `json:"humidity,omitempty"`
	Temp     float64 `json:"temp,omitempty"`
}

type requestInfo struct {
	Command string        `json:"command"`
	Params  requestObject `json:"params,omitempty"`
}

type requestReturn struct {
	Command string        `json:"command"`
	Error   requestError  `json:"error,omitempty"`
	Object  requestObject `json:"object,omitempty"`
}

func getCityFromStdin() string {
	myscanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("Enter the city you interesting in")
		myscanner.Scan()
		if myscanner.Text() == "" {
			break
		}
		return myscanner.Text()
	}
	return myscanner.Text()

}

func getDateTimeFromStdin() (timestamp time.Time) {

	for {
		myscanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Enter the date and time you interesting in\nFormat: DD-MM-YYYY HH:MM:SS")
		var datetime string
		myscanner.Scan()
		datetime = myscanner.Text()
		//fmt.Println(datetime)
		timestamp, err := time.ParseInLocation("02-01-2006 15:04:05", datetime, time.Local)
		if err != nil {
			fmt.Println("You entered incorrect time,", err)
		} else {
			//fmt.Println("requested time is", timestamp)
			return timestamp
		}
	}
	return timestamp
}

func sendDataToSocket(conn net.Conn, param requestInfo) error {
	requestByteArray := make([]byte, 4)
	var amount uint32
	myByteArray, err := json.Marshal(param)
	if err != nil {
		fmt.Println("error marshaling:", err)
		return err
	}
	amount = uint32(len(myByteArray))
	binary.BigEndian.PutUint32(requestByteArray, amount)
	for _, bytePart := range myByteArray {
		requestByteArray = append(requestByteArray, bytePart)
	}
	_, err = conn.Write(requestByteArray)
	if err != nil {
		fmt.Println("Error sending data:", err)
		return err
	}
	return nil
}

func receiveDataFromSocket(conn net.Conn) (forecastNow requestReturn, err error) {
	buff := make([]byte, 1024)
	_, err = conn.Read(buff)
	if err != nil {
		if err.Error() == "EOF" {
			fmt.Println("Connection with server lost")
			return forecastNow, err
		}
		fmt.Println("Error receiving data:", err.Error())
		return forecastNow, err
	}

	amount := binary.BigEndian.Uint32([]byte(buff[0:4]))
	err = json.Unmarshal(buff[4:amount+4], &forecastNow)
	if err != nil {
		fmt.Println("Unmarshal error: ", err)
		return forecastNow, err
	}
	return forecastNow, nil
}

func needContinue(conn net.Conn, param requestInfo) bool {
	fmt.Println("Press ENTER to continue or type Exit to close connection")
	myscanner := bufio.NewScanner(os.Stdin)
	myscanner.Scan()
	if myscanner.Text() == "Exit" {
		param.Command = "closeConnection"
		requestByteArray := make([]byte, 4)
		myByteArray, err := json.Marshal(param)
		if err != nil {
			fmt.Println("error marshaling:", err)
			return true
		}
		amount := uint32(len(myByteArray))
		binary.BigEndian.PutUint32(requestByteArray, amount)
		for _, bytePart := range myByteArray {
			requestByteArray = append(requestByteArray, bytePart)
		}
		_, err = conn.Write(requestByteArray)
		return false
	}
	return true
}

func GetInfoFromServer(conn net.Conn, param requestInfo) bool {
	param.Params.City = getCityFromStdin()
	param.Params.Date = getDateTimeFromStdin().Unix()
	fmt.Println("\nTrying to search closest forecast for", param.Params.City, "at", time.Unix(param.Params.Date, 0).Local())
	err := sendDataToSocket(conn, param)
	if err != nil {
		return false
	}
	forecastNow, err := receiveDataFromSocket(conn)
	if err != nil {
		return false
	}

	if forecastNow.Error.Message != "" {
		fmt.Println("Error searching data for", param.Params.City, "\nError:", forecastNow.Error.Message)
		return true
	}
	fmt.Println("Closest forecast is at", time.Unix(forecastNow.Object.Date, 0), "in", forecastNow.Object.City, "Temperature is", forecastNow.Object.Temp)

	return needContinue(conn, param)
}

// correct date example for test 27-07-2018 23:59:15
func main() {

	param := new(requestInfo)
	param.Command = "GetWeather" //set default command
	var connectAddr string
	myscanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter IP address for connection to or press ENTER to connect to server in Localhost")
	myscanner.Scan()
	connectAddr = myscanner.Text()
	connectAddr += ":7777"
	fmt.Println("Trying to connect to", connectAddr)
	conn, err := net.Dial("tcp", connectAddr)
	if err != nil {
		fmt.Println("error connection: ", err)
		return
	} else {
		fmt.Println("Connected success to", connectAddr)
	}
	defer func() {
		conn.Close()
		fmt.Println("Disconnecting")
	}()

	for {

		continueRequesting := GetInfoFromServer(conn, *param)
		if !continueRequesting {
			return
		}
	}
}
