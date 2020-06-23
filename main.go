package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Commune struct {
	Nom          string   `json:"nom"`
	Departement  string   `json:"departement,omitempty"`
	Region       string   `json:"region,omitempty"`
	CodesPostaux []string `json:"codesPostaux"`
	Location     Location `json:"location"`
}

type Location struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type Departement struct {
	Nom  string
	Code string
}

type Region struct {
	Nom  string
	Code string
}

type Insee struct {
	Code string `json:"code"`
}

type CommuneResponse struct {
	Type       string `json:"type"`
	Properties struct {
		Nom             string   `json:"nom"`
		Code            string   `json:"code"`
		CodesPostaux    []string `json:"codesPostaux"`
		CodeDepartement string   `json:"codeDepartement"`
		CodeRegion      string   `json:"codeRegion"`
		Population      int      `json:"population"`
	} `json:"properties"`
	Geometry struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"geometry"`
}

func main() {
	fmt.Println("Recherche des communes............")
	communeCh, errorCh := communesWorker()
	communes := readCities(communeCh, errorCh)
	writeIntoJSON(communes)
	fmt.Println("Fin de recherche des communes")
}

func fetchCodeInsee() ([]Insee, error) {
	resp, err := http.Get("https://geo.api.gouv.fr/communes?fields=nom,code,codesPostaux,codeDepartement,codeRegion,population&format=json&geometry=centre")
	if err != nil {
		return []Insee{}, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []Insee{}, err
	}
	insees := []Insee{}
	err = json.Unmarshal(body, &insees)
	return insees, err
}

func fetchDepartements() ([]Departement, error) {
	resp, err := http.Get("https://geo.api.gouv.fr/departements?fields=nom,code,codeRegion")
	if err != nil {
		return []Departement{}, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []Departement{}, err
	}
	departements := []Departement{}
	err = json.Unmarshal(body, &departements)
	return departements, err
}

func fetchRegions() ([]Region, error) {
	resp, err := http.Get("https://geo.api.gouv.fr/regions?fields=nom,code")
	if err != nil {
		return []Region{}, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []Region{}, err
	}
	regions := []Region{}
	err = json.Unmarshal(body, &regions)
	return regions, err
}

func communesWorker() (<-chan Commune, <-chan error) {
	communeCh := make(chan Commune, 10)
	errorCh := make(chan error, 2)
	insees, err := fetchCodeInsee()
	if err != nil {
		log.Fatalln(err)
	}
	departements, err := fetchDepartements()
	if err != nil {
		log.Fatalln(err)
	}
	regions, err := fetchRegions()
	if err != nil {
		log.Fatalln(err)
	}
	go func() {
		wg := sync.WaitGroup{}
		for _, insee := range insees {
			sleeping()
			wg.Add(1)
			go geoAPIGouvFr(&wg, insee.Code, departements, regions, communeCh, errorCh)
		}
		wg.Wait()
		close(errorCh)
		close(communeCh)
	}()
	return communeCh, errorCh
}

func geoAPIGouvFr(wg *sync.WaitGroup, codeInsee string, departements []Departement, regions []Region, communeCh chan<- Commune, errorCh chan<- error) {
	defer wg.Done()
	url := fmt.Sprintf("https://geo.api.gouv.fr/communes/%s?fields=nom,code,codesPostaux,codeDepartement,codeRegion,population&format=geojson&geometry=centre", codeInsee)
	resp, err := http.Get(url)
	if err != nil || resp.StatusCode == 404 {
		errorCh <- fmt.Errorf("%s", codeInsee)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errorCh <- fmt.Errorf("%s", codeInsee)
		return
	}
	cs := CommuneResponse{}
	if err = json.Unmarshal(body, &cs); err != nil {
		errorCh <- fmt.Errorf("%s", codeInsee)
		return
	}
	c := Commune{}
	c.Nom = cs.Properties.Nom
	if cs.Properties.CodeDepartement != "" {
		for _, dep := range departements {
			if dep.Code == cs.Properties.CodeDepartement {
				c.Departement = dep.Nom
			}
		}
	}
	if cs.Properties.CodeRegion != "" {
		for _, reg := range regions {
			if reg.Code == cs.Properties.CodeRegion {
				c.Region = reg.Nom
			}
		}
	}
	if len(cs.Properties.CodesPostaux) > 0 {
		c.CodesPostaux = cs.Properties.CodesPostaux
	}
	if len(cs.Geometry.Coordinates) > 0 {
		c.Location.Type = "Point"
		c.Location.Coordinates = append(c.Location.Coordinates, cs.Geometry.Coordinates[1])
		c.Location.Coordinates = append(c.Location.Coordinates, cs.Geometry.Coordinates[0])
	}
	communeCh <- c
}

func readCities(communeCh <-chan Commune, errorCh <-chan error) []Commune {
	ticker := time.NewTicker(15 * time.Minute)
	var communes []Commune
	var communeCounter int
	for communeCh != nil || errorCh != nil {
		select {
		case commune, ok := <-communeCh:
			if !ok {
				communeCh = nil
				continue
			}
			communes = append(communes, commune)
			communeCounter += 1
		case err, ok := <-errorCh:
			if !ok {
				errorCh = nil
				continue
			}
			logErrors(err)
		case <-ticker.C:
			t := time.Now().UTC()
			fmt.Printf("[%s] %d communes ont été traitées\n", t.Format("02-01-2006 15:04:05"), communeCounter)
		}
	}
	ticker.Stop()

	sort.SliceStable(communes, func(i, j int) bool {
		if len(communes[i].CodesPostaux) == 0 || len(communes[j].CodesPostaux) == 0 {
			return true
		}
		o, err := strconv.ParseInt(strings.TrimLeft(communes[i].CodesPostaux[0], "0"), 10, 64)
		if err != nil {
			return true
		}
		k, err := strconv.ParseInt(strings.TrimLeft(communes[j].CodesPostaux[0], "0"), 10, 64)
		if err != nil {
			return true
		}
		return o < k
	})
	return communes
}

func writeIntoJSON(communes []Commune) {
	jsonData, err := json.MarshalIndent(communes, "", " ")
	if err != nil {
		log.Fatalln(err)
	}
	filename := fmt.Sprintf("communesFR_%d-%d-%d.json", time.Now().Day(), int(time.Now().Month()), time.Now().Year())
	err = ioutil.WriteFile(filename, jsonData, 0644)
	if err != nil {
		log.Fatalln(err)
	}
}

func sleeping() {
	rand.Seed(time.Now().UnixNano())
	sleeper := rand.Intn(75-35) + 35
	time.Sleep(time.Duration(sleeper) * time.Millisecond)
}

func logErrors(er error) {
	file, err := os.OpenFile("log-errors.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	datawriter := bufio.NewWriter(file)
	_, err = datawriter.WriteString(er.Error() + "\n")
	if err != nil {
		log.Fatalf("failed writing file: %s", err)
	}
	datawriter.Flush()
	file.Close()
}
