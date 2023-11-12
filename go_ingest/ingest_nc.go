package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/batchatco/go-native-netcdf/netcdf"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

func getFYDirsAndFiles(dataPath string) []string {
	dirs, err := os.ReadDir(dataPath)
	if err != nil {
		fmt.Println("Failed to read dir: " + dataPath)
		panic(err)
	}

	var files []string
	for _, dir := range dirs {
		if dir.Name() == ".DS_Store" {
			continue
		}

		dirFiles, err := os.ReadDir(filepath.Join(dataPath, dir.Name()))
		if err != nil {
			fmt.Println("Failed to read dir: " + filepath.Join(dataPath, dir.Name()))
			panic(err)
		}
		for _, file := range dirFiles {
			if file.Name()[len(file.Name())-3:] == ".nc" {
				files = append(files, filepath.Join(dataPath, dir.Name(), file.Name()))
			}
		}
	}

	return files
}

/*
Unmasks singular value from int16 to float64 (fills empties as well)
*/
func unmaskValue(value int16) float64 {
	MISSING_VALUE := 32767
	SCALE_FACTOR := 0.01
	ADD_OFFSET := -15.0
	EMPTY_VALUE := -1000.0

	var masked float64
	if value == int16(MISSING_VALUE) {
		return EMPTY_VALUE
	}

	floatedValue := float64(value)
	masked = (floatedValue * SCALE_FACTOR) + ADD_OFFSET
	return masked
}

/*
Fetches lat, lon, and daily_mean_palmer_drought_severity_index from .nc file
*/
func getVariables(nc api.Group) ([]float64, []float64, [][][]int16) {
	// Read the NetCDF variable from the file
	rawLatitudes, _ := nc.GetVariable("lat")
	if rawLatitudes == nil {
		panic("rawLatitudes not found")
	}

	rawLongitudes, _ := nc.GetVariable("lon")
	if rawLongitudes == nil {
		panic("rawLatitudes not found")
	}

	rawDailyMeanPalmerDroughtSeverityIndex, _ := nc.GetVariable("daily_mean_palmer_drought_severity_index")
	if rawDailyMeanPalmerDroughtSeverityIndex == nil {
		panic("rawDailyMeanPalmerDroughtSeverityIndex not found")
	}

	// Cast the data into a Go type we can use
	latitudes, has := rawLatitudes.Values.([]float64)
	if !has {
		panic("failed to cast latitudes")
	}

	longitudes, has := rawLongitudes.Values.([]float64)
	if !has {
		panic("Failed to cast longitudes")
	}

	droughtSeverities := rawDailyMeanPalmerDroughtSeverityIndex.Values.([][][]int16)
	if !has {
		panic("Failed to cast droughtSeverities")
	}

	return latitudes, longitudes, droughtSeverities
}

func convertFloatToString(val float64) string {
	return strconv.FormatFloat(val, 'f', 15, 64)
}

/*
Ingests .nc files and outputs clean .csv files for ease of use for the ML model.
*/
func ingestNCFile(filePath string) {
	ncFile, err := netcdf.Open(filePath)
	if err != nil {
		panic(err)
	}

	latitudes, longitudes, maskedDroughtSeverities := getVariables(ncFile)
	ncFile.Close()

	csvFilePath := strings.Replace(filePath, ".nc", ".csv", 1)
	csvFilePath = strings.Replace(csvFilePath, filepath.Base(csvFilePath), "go_"+filepath.Base(csvFilePath), 1)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		// panic("Failed to create csv file", err)
		panic(err)
	}
	defer csvFile.Close()

	EASTERN_BORDER_OF_NEW_MEXICO_LONGITUDE := -103.0
	EMPTY_VALUE := -1000.0
	for latIndex, lat := range latitudes {
		lines := ""
		for lonIndex, lon := range longitudes {
			if lon > EASTERN_BORDER_OF_NEW_MEXICO_LONGITUDE {
				break
			}

			line := fmt.Sprintf("%s %s", convertFloatToString(lat), convertFloatToString(lon))
			for _, day := range maskedDroughtSeverities {
				droughtSeverity := unmaskValue(day[latIndex][lonIndex])
				if droughtSeverity != EMPTY_VALUE {
					line += fmt.Sprintf(",%s", convertFloatToString(droughtSeverity))
				} else {
					line += ","
				}
			}

			if !strings.Contains(line, ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,") {
				line += "\n"
				lines += line
			}
		}
		csvFile.WriteString(lines)

		if latIndex%100 == 0 {
			msg := fmt.Sprintf("%s at %s of 584", filepath.Base(csvFilePath), strconv.Itoa(latIndex))
			fmt.Println(msg)
		}
	}
}

func main() {
	DATA_PATH := "../../water-supply-forecast-rodeo-runtime/data/pdsi"
	start := time.Now()

	files := getFYDirsAndFiles(DATA_PATH)
	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			ingestNCFile(file)
		}(file)
	}
	wg.Wait()

	elapsed := time.Since(start)
	msg := fmt.Sprintf("Total runtime: %s seconds", strconv.FormatFloat(elapsed.Seconds(), 'f', 2, 64))
	fmt.Println(msg)
}
