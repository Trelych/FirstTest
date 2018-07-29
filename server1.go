package main

import (
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
)

var (
	owmApiKey           = "6a1fbe6d745b0ed366e29c17c4f0624d"
	owmGroupRequestAddr = "http://api.openweathermap.org/data/2.5/group?id="
	owmApiAddParam      = "&units=metric&appid="
	hPaToRussian        = 0.750062
	citiesForRequest    = []int{2643741, 524901, 5391959, 1816670}
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

/* can be needed in future
type sitiesTable struct {
	Id             int64
	Name           string
	Country        string
	Weather_api_id int64
}
*/
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

//get and parse JSON using URL and given structure as parameter
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

//make the url string to request weather data from OWM with selected pack of cities
func makeOWMApiRequestString() (result string) {
	result = ""
	result += owmGroupRequestAddr
	for i := 0; i < len(citiesForRequest); i++ {
		result += strconv.Itoa(int(citiesForRequest[i]))
		if i != len(citiesForRequest)-1 {
			result += ","
		}
	}
	result += owmApiAddParam
	result += owmApiKey
	return result

}

//getting needed struct with database request string
func getRequestedForecastDataFomPG(db *sql.DB, query string) (forecastResult []*forecast, err error) {
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
	err = GetJsonFromUrl(makeOWMApiRequestString(), &packOfCities)

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

//check the requested command from client and process the chosen command
func processRequest(request requestInfo, db *sql.DB) (result requestReturn, needClose bool) {
	switch request.Command {
	case "GetWeather":
		//fmt.Println("!Getting weather info")
		result = getCorrectForecastData(db, request)

	case "closeConnection":
		//fmt.Println("Closing connection")
		return result, true
	}
	return result, false
}

func makeQueryStringForClientRequest(city string, time int64) string {
	return "(select f.*, s.name from forecasts f inner join sities s on f.city_id = s.id and s.name = '" + city + "' and f.time < " + strconv.FormatInt(time, 10) + " order by f.time desc limit 1) union (select f.*, s.name from forecasts f inner join sities s on f.city_id = s.id and s.name = '" + city + "' and f.time >= " + strconv.FormatInt(time, 10) + " order by f.time asc limit 1) limit 2"
}

//get up to 2 nearest data from client request and return 1 most closest forecast
func getCorrectForecastData(db *sql.DB, request requestInfo) (result requestReturn) {
	forecastNow, err := getRequestedForecastDataFomPG(db, makeQueryStringForClientRequest(request.Params.City, request.Params.Date))
	if err != nil {
		fmt.Println(err)
	}
	closest := -1
	//if have 2 nearest, check closest, get it's number in array
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

func fillSitiesTable(db *sql.DB) (err error) {
	var packOfCities cityPackData
	err = GetJsonFromUrl(makeOWMApiRequestString(), &packOfCities)

	if err != nil {
		fmt.Println("error getting json:\n", err)
		return err
	}
	for i := range packOfCities.CityPackData {

		_, err := db.Exec("INSERT INTO sities VALUES($1, $2, $3, $4)", i, packOfCities.CityPackData[i].Name, packOfCities.CityPackData[i].Osys.Country, packOfCities.CityPackData[i].Id)
		if err != nil {
			fmt.Println("Error inserting:", err)
			return err
		}

	}
	return nil
}

func handleConnection(conn net.Conn, db *sql.DB) {
	name := conn.RemoteAddr().String()
	fmt.Println("New connection from", name)
	var amount uint32
	var request requestInfo

	defer func() {
		conn.Close()
		fmt.Println("disconnected")
	}()

	for {
		buff := make([]byte, 1024)
		_, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Error reading", err)
		}
		amount = binary.BigEndian.Uint32([]byte(buff[0:4]))
		err = json.Unmarshal(buff[4:amount+4], &request)
		if err != nil {
			fmt.Println("Unmarshal error: ", err)
			break
		}
		responce, needClose := processRequest(request, db)
		if needClose == true {
			fmt.Println("closing connection")
			break
		}
		//creating array with size of 4 bytes to begin store size of marshaled json response string
		sendByteArray := make([]byte, 4)
		buff, err = json.Marshal(responce)
		//check the marshaled byte array size and put it into 4 first bytes of response
		amount = uint32(len(buff))
		binary.BigEndian.PutUint32(sendByteArray, amount)
		//put marshaled data to same array and send it to client
		for _, x := range buff {
			sendByteArray = append(sendByteArray, x)
		}
		_, err = conn.Write(sendByteArray)
		if err != nil {
			fmt.Println("error sending:", err)
		}
	}

}

func main() {
	db, err := sql.Open("postgres", "postgres://postgres:asecurepassword@localhost/weather?sslmode=disable")
	if err != nil {
		fmt.Println(err)
	}
	//fill the table sities for first time data
	err = fillSitiesTable(db)
	if err != nil {
		fmt.Println(err)
	}

	//permanent collect data and store it into database
	go func() {
		getAndSubmitForecastData(db)
		time.Sleep(time.Hour)
	}()

	//begin to listen tcp socket connection on localhost port 7777
	listener, err := net.Listen("tcp", ":7777")
	if err != nil {
		fmt.Println("Error listening socket\n", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accept connection", err)
		}
		//for multiple connection, run handler in goroutine
		go handleConnection(conn, db)
	}

}
