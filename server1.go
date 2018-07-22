package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"time"
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
	Error string `json:"error,omitempty"`
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

func getForecastData(db *sql.DB) (err error) {
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

func main() {
	db, err := sql.Open("postgres", "postgres://postgres:asecurepassword@localhost/weather?sslmode=disable")
	if err != nil {
		fmt.Println(err)
	}
	//_ := db //чтобы не мешалось

	param := new(requestInfo)
	param.Command = "GetWeather"
	param.Params.City = "Moscow"
	param.Params.Date = 1532284783

	query := "select f.*, s.name from forecasts f inner join sities s on f.city_id = s.id and s.name = '" + param.Params.City + "' and f.time > " + strconv.Itoa(int(param.Params.Date)) + " order by f.time asc limit 1"

	forecastNow, err := readRowsFromPG(db, query)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Closed forecast for", time.Unix(forecastNow[0].Time, 0), "City", forecastNow[0].City_name, "Temperature is", forecastNow[0].Temp)

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
