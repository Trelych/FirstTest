package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"
	//	"bytes"
)

var (
	appid        = "6a1fbe6d745b0ed366e29c17c4f0624d"
	apiAddr      = "http://api.openweathermap.org/data/2.5/group?id=2643741,524901,5391959,1816670&units=metric&appid="
	hPaToRussian = 0.750062
)

type coord struct {
	Lon float64 `json:"lon"`
	Lat float64 `json:"lat"`
}

type oweather struct {
	Id          int64  `json:"id"`
	Main        string `json:"main"`
	Description string `json:"description"`
	Icon        string `json:"icon"`
}

type owmain struct {
	Temp       float64 `json:"temp"`
	Pressure   float64 `json:"pressure"`
	Humidity   float64 `json:"humidity"`
	Temp_min   float64 `json:"temp_min"`
	Temp_max   float64 `json:"temp_max"`
	Sea_level  float64 `json:"sea_level"`
	Grnd_level float64 `json:"grnd_level"`
}

type wind struct {
	Speed float64 `json:"speed"`
	Deg   float64 `json:"deg"`
}

type clouds struct {
	All float64 `json:"all"`
}

type rain struct {
	Precipitation int64 `json:"3h,omitempty"`
}

type snow struct {
	Precipitation int64 `json:"3h,omitempty"`
}

type sys struct {
	Otype   int64   `json:"type"`
	Id      int64   `json:"id"`
	Message float64 `json:"message"`
	Country string  `json:"country"`
	Sunrise int64   `json:"sunrise"`
	Sunset  int64   `json:"sunset"`
}

type openWeatherFull struct {
	Ocoord   coord      `json:"coord"`
	Oweather []oweather `json:"weather"`
	Base     string     `json:"base"`
	Omain    owmain     `json:"main"`
	Owind    wind       `json:"wind"`
	Oclouds  clouds     `json:"clouds"`
	Orain    rain       `json:"rain,omitempty"`
	Osnow    snow       `json:"snow,omitempty"`
	Dt       int64      `json:"dt"`
	Osys     sys        `json:"sys"`
	Id       int64      `json:"id"`
	Name     string     `json:"name"`
	Cod      int64      `json:"cod"`
}

type cityPackData struct {
	Cnt          int64             `json:"cnt"`
	CityPackData []openWeatherFull `json:"list"`
}

type sities struct {
	Id             int64
	Name           string
	Country        string
	Weather_api_id int64
}

type forecast struct {
	Id        int64 //`omitempty`
	Time      int64
	Temp      float64
	Humidity  float64
	Pressure  float64
	City_id   int
	City_name string //`omitempty`
}

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

func GetJsonFromUrl(url string, jsonObject interface{}) (error error) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("=====ERROR HTTP.GET=====")
		fmt.Println(err)
		fmt.Println("========================")
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("=====ERROR READALL =====")
		fmt.Println(err)
		fmt.Println("========================")
		return err
	}
	fmt.Println(string(result))
	err = json.Unmarshal(result, &jsonObject)
	if err != nil {
		fmt.Println("=====ERROR Unmarshal =====")
		fmt.Println(err)
		fmt.Println("========================")
		return err
	}
	fmt.Println(jsonObject)
	return nil
}

func makeRequestString(param ...string) (result string) {
	result = ""
	for _, x := range param {
		result += x
	}
	fmt.Println(result)
	return result

}

func readRowsFromPG(db *sql.DB, query string) (forecastResult []*forecast, err error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultForecast := make([]*forecast, 0)

	for rows.Next() {
		line := new(forecast)
		err := rows.Scan(&line.Id, &line.Time, &line.Temp, &line.Humidity, &line.Pressure, &line.City_id, &line.City_name)
		if err != nil {
			return nil, err
		}
		resultForecast = append(resultForecast, line)

	}
	if err = rows.Err(); err != nil {
		fmt.Println(err)
		return nil, err
	}

	return resultForecast, nil

}

func getAndSubmitForecastData(db *sql.DB) (err error) {
	var packOfCities cityPackData
	err = GetJsonFromUrl(makeRequestString(apiAddr, appid), &packOfCities)

	if err != nil {
		fmt.Println("error getting json:\n", err)
		return err
	}
	for i := range packOfCities.CityPackData {

		result, err := db.Exec("INSERT INTO forecasts VALUES(DEFAULT, $1, $2, $3, $4, (select id from sities where weather_api_id = $5))", packOfCities.CityPackData[i].Dt, (math.Round(packOfCities.CityPackData[i].Omain.Temp*100) / 100), (math.Round(packOfCities.CityPackData[i].Omain.Humidity*100) / 100), (math.Round((packOfCities.CityPackData[i].Omain.Pressure * hPaToRussian * 100) / 100)), packOfCities.CityPackData[i].Id)
		if err != nil {
			fmt.Println("Error inserting:", err)
			return err
		} else {
			fmt.Println(result)
		}

	}
	return nil
}

func processRequest(request requestInfo, db *sql.DB) (result requestReturn) {
	switch request.Command {
	case "GetWeather":
		fmt.Println("!Getting weather info")
		result = getCorrectForecastData(db, request)

	case "closeConnection":
		fmt.Println("Closing connection")
	}
	return result
}

func makeQueryStringForClientRequest(city string, time int64) string {
	return "(select f.*, s.name from forecasts f inner join sities s on f.city_id = s.id and s.name = '" + city + "' and f.time < " + strconv.FormatInt(time, 10) + " order by f.time desc limit 1) union (select f.*, s.name from forecasts f inner join sities s on f.city_id = s.id and s.name = '" + city + "' and f.time >= " + strconv.FormatInt(time, 10) + " order by f.time asc limit 1) limit 2"
}

func getCorrectForecastData(db *sql.DB, request requestInfo) (result requestReturn) {
	forecastNow, err := readRowsFromPG(db, makeQueryStringForClientRequest(request.Params.City, request.Params.Date))
	if err != nil {
		fmt.Println(err)
	}
	closest := -1
	if len(forecastNow) > 1 {
		if forecastNow[1].Time-request.Params.Date <= request.Params.Date-forecastNow[0].Time {
			closest = 1
		} else {
			closest = 0
		}
	} else {
		if len(forecastNow) == 1 {
			closest = 0
		}
	}

	result.Command = request.Command

	if closest < 0 {
		result.Error.Message = "Data for city " + request.Params.City + " is not found."
	} else {
		result.Object.Date = forecastNow[closest].Time
		result.Object.City = forecastNow[closest].City_name
		result.Object.Pressure = forecastNow[closest].Pressure
		result.Object.Humidity = forecastNow[closest].Humidity
		result.Object.Temp = forecastNow[closest].Temp
	}
	return result
}

func handleConnection(conn net.Conn, db *sql.DB) {
	//conn.SetDeadline(time.Now()+(10*time.Second))
	name := conn.RemoteAddr().String()
	fmt.Println("New connection from", name)
	var returnByteArray []byte
	var amount uint32
	amount = 0
	var request requestInfo
	var responce requestReturn
	defer func() {
		conn.Close()
		fmt.Println("disconnected")
	}()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		myByteArray := scanner.Bytes()
		fmt.Println("Received array of Bytes\nAmount of bytes received", len(myByteArray))
		fmt.Println("Array is", string(myByteArray))
		err := json.Unmarshal(myByteArray[4:], &request)
		if err != nil {
			fmt.Println("Unmarshal error: ", err)
			break
		} else {
			responce = processRequest(request, db)
			myByteArray, err = json.Marshal(responce)
			binary.LittleEndian.PutUint32(returnByteArray, amount)
			for _, x := range myByteArray {
				returnByteArray = append(returnByteArray, x)
			}
			conn.Write(returnByteArray)

		}

	}
}

func main() {
	db, err := sql.Open("postgres", "postgres://postgres:asecurepassword@localhost/weather?sslmode=disable")
	if err != nil {
		fmt.Println(err)
	}

	//test stuct to check the right
	param := new(requestInfo)
	param.Command = "GetWeather"
	param.Params.City = "Moscow"
	param.Params.Date = 1532255871
	//processRequest(param)

	//query := "select f.*, s.name from forecasts f inner join sities s on f.city_id = s.id and s.name = '" + param.Params.City + "' and f.time > " + strconv.Itoa(int(param.Params.Date)) + " order by f.time desc limit 1"
	/*
		forecastNow, err := readRowsFromPG(db, makeQueryStringForClientRequest(param.Params.City, param.Params.Date))
		if err != nil {
			fmt.Println(err)
		}
		closest := -1
		if len(forecastNow) > 1 {
			if forecastNow[1].Time - param.Params.Date <= param.Params.Date - forecastNow[0].Time {
				closest = 1
			} else {
				closest = 0
			}
		} else {
			if len(forecastNow) == 1 {
				closest = 0
			}
		}
	*/
	forecastNow := getCorrectForecastData(db, *param)
	//fmt.Println("result query count", len(forecastNow))
	fmt.Println("Closed forecast for", time.Unix(forecastNow.Object.Date, 0), "City", forecastNow.Object.City, "Temperature is", forecastNow.Object.Temp)

	listener, err := net.Listen("tcp", ":7777")
	if err != nil {
		fmt.Println("Error listening socket\n", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accept connection", err)
		}
		go handleConnection(conn, db)
	}

	/* infinity collect data from OpenWeatherMap
	for {
		err := getForecastData(db)
		if err != nil {
			fmt.Println(err)
		} else {
			time.Sleep(time.Hour)
		}

	}
	*/

	/*
		nowForecast := make([]*forecast, 0)
		requestString := "SELECT f.*, s2.name FROM forecasts f INNER JOIN sities s2 ON f.city_id = s2.id"
		nowForecast, err = readRowsFromPG(db, requestString)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(nowForecast)

		for _, line := range nowForecast {
			fmt.Println("\n\nForecast for city #", line.City_name, "at", time.Unix(line.Time, 0), "\nTemperature", line.Temp, "C degrees\nHumidity =", line.Humidity, "%\nPressure =", line.Pressure, "mmHg")
		}
	*/

}
