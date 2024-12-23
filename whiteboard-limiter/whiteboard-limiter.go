// Внешние запросы дёргают handle, нужно ограничить запросы

/*
___Facts___
 handle() is called N times/minutes
 expensive() does expensive operations
*/

/*
___Requirements__
The available budget is "100/minutes and 800/hours". Anything beyond should be failed.
*/

package main

import (
	"errors"
	"sync"
	"time"
)

const minutesLimit = 100 // 100000 <-? Как я поменял бы код если запросов было бы на 3 порядка больше
const hoursLimit = 800   // 8000000

var mx sync.Mutex

// checkTime trims timeSeries and returns true if it is ok to make another request
func checkTime(timeSeries *[]time.Time, limit int, now time.Time, offset time.Duration) bool {
	mx.Lock()
	for len(*timeSeries) != 0 && (*timeSeries)[0].Add(offset).Before(now) {
		*timeSeries = (*timeSeries)[1:]
	}
	ok := len(*timeSeries) < limit
	mx.Unlock()

	return ok
}

var callsMin, callsHour []time.Time

func expensive() (any, error) { return nil, nil }

func handle() (any, error) {
	now := time.Now()

	if !checkTime(&callsMin, minutesLimit, now, time.Minute) {
		return nil, errors.New("too many requests per minute")
	}
	if !checkTime(&callsHour, hoursLimit, now, time.Hour) {
		return nil, errors.New("too many requests per hour")
	}

	callsMin = append(callsMin, now)
	callsHour = append(callsHour, now)

	return expensive()
}

func main() {
	// <-? Что нужно добавить чтобы запускать это приложение на серверах / в облаке
	// take argc and argv
	// SETTING_1
	// add logging: graylog, datadog

	// <-? Кубернейтрис, деплой, мой опыт деплоя
	// scp || ssh
	// services restart
}
