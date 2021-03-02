package main

import (
	cu "github.com/achelovekov/collectorutils"
	"fmt"
	"os"
	"io/ioutil"
	"encoding/json"	
	"net/http"
	"log"
	"strings"
	"time"
	"sync"
)

type INSAPIResponseJSONText struct {
	INSAPI struct {
		Outputs struct {
			Output struct {
				Body  string `json:"body"`
				Code  string `json:"code"`
				Input string `json:"input"`
				Msg   string `json:"msg"`
			} `json:"output"`
		} `json:"outputs"`
		Sid     string `json:"sid"`
		Type    string `json:"type"`
		Version string `json:"version"`
	} `json:"ins_api"`
}

type INSAPIResponseJSONNative struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Body map[string]interface{} `json:"body"`
	} `json:"result"`
	ID int `json:"id"`
}

type Inventory []struct {
	Host struct {
		URL      string `json:"url"`
		Hostname string `json:"hostname"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"host"`
}

type Config struct {
	ESHost              string `json:"ESHost"`
	ESPort              string `json:"ESPort"`
	ESIndex             string `json:"ESIndex"`
	INSPathsDefinitions string `json:"INSPathsDefinitions"`
	FilterFile          string `json:"FilterFile"`
	EnrichFile          string `json:"EnrichFile"`
}

type Request struct {
	URL string
	Hostname string
	RequestString string
	Username string
	Password string	 
}

type INSGeneric struct {
	Config   Config
	INSPaths INSPaths
	ESClient cu.ESClient
	Filter   cu.Filter
	Enrich   cu.Enrich
	Mode     int
}

type INSPathsDefinition struct {
	cu.PathDefinition
	JSONNative bool   `json:"jsonNative"`
	JSONText   bool   `json:"jsonText"`
}

type INSPathsDefinitions []INSPathsDefinition

type INSPath struct {
	Key string
	Paths []cu.Path
	JSONNative bool
	JSONText   bool
}

type INSPaths []INSPath

func LoadINSPaths(fileName string) INSPaths {

	var INSPathsDefinitions INSPathsDefinitions
	INSPaths := make(INSPaths,0)

	INSPathDefinitionsFile, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer INSPathDefinitionsFile.Close()

	INSPathDefinitionsFileBytes, err := ioutil.ReadAll(INSPathDefinitionsFile)
	if err != nil {
		fmt.Println(err)
	}

	err = json.Unmarshal(INSPathDefinitionsFileBytes, &INSPathsDefinitions)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range INSPathsDefinitions {

		var INSPath INSPath
		var paths []cu.Path

		INSPath.JSONNative = v.JSONNative
		INSPath.JSONText = v.JSONText
		INSPath.Key = v.Key

		for _, v := range v.Paths {
			pathFile, err := os.Open(v.Path)
			if err != nil {
				fmt.Println(err)
			}
			defer pathFile.Close()

			pathFileBytes, _ := ioutil.ReadAll(pathFile)
			var path cu.Path
			err = json.Unmarshal(pathFileBytes, &path)
			if err != nil {
				fmt.Println(err)
			}
			paths = append(paths, path)
		}

		INSPath.Paths = paths

		INSPaths = append(INSPaths, INSPath)
	}

	return INSPaths
}


func worker(src map[string]interface{}, ESClient cu.ESClient, ESIndex string, path cu.Path, mode int, filter cu.Filter, enrich cu.Enrich, hostname string) {
	
	var pathIndex int
	header := make(map[string]interface{})
	buf := make([]map[string]interface{}, 0)
	pathPassed := make([]string, 0)

	cu.FlattenMap(src, path, pathIndex, pathPassed, mode, header, &buf, filter, enrich)
	for _,v := range(buf){
		v["node_id_str"] = hostname
	}
	cu.ESPush(ESClient, ESIndex, buf)
}

func Initialize(configFile string, inventoryFile string) (cu.ESClient, Config, Inventory, INSPaths, cu.Filter, cu.Enrich) {

	var Config Config
	var Inventory Inventory
	var INSPaths INSPaths
	var Filter cu.Filter
	var Enrich cu.Enrich

	ConfigFile, err := os.Open(configFile)
	if err != nil {
		fmt.Println(err)
	}
	defer ConfigFile.Close()

	ConfigFileBytes, _ := ioutil.ReadAll(ConfigFile)

	err = json.Unmarshal(ConfigFileBytes, &Config)
	if err != nil {
		fmt.Println(err)
	}

	InventoryFile, err := os.Open(inventoryFile)
	if err != nil {
		fmt.Println(err)
	}
	defer InventoryFile.Close()

	InventoryFileBytes, _ := ioutil.ReadAll(InventoryFile)

	err = json.Unmarshal(InventoryFileBytes, &Inventory)
	if err != nil {
		fmt.Println(err)
	}

	INSPaths = LoadINSPaths(Config.INSPathsDefinitions)

	FilterFile, err := os.Open(Config.FilterFile)
	if err != nil {
		fmt.Println(err)
	}
	defer FilterFile.Close()

	FilterFileBytes, _ := ioutil.ReadAll(FilterFile)

	err = json.Unmarshal(FilterFileBytes, &Filter)
	if err != nil {
		fmt.Println(err)
	}

	EnrichFile, err := os.Open(Config.EnrichFile)
	if err != nil {
		fmt.Println(err)
	}
	defer ConfigFile.Close()

	EnrichFileBytes, _ := ioutil.ReadAll(EnrichFile)

	err = json.Unmarshal(EnrichFileBytes, &Enrich)
	if err != nil {
		fmt.Println(err)
	}

	ESClient, error := cu.ESConnect(Config.ESHost, Config.ESPort)
	if error != nil {
		log.Fatalf("error: %s", error)
	}

	return ESClient, Config, Inventory, INSPaths, Filter, Enrich
}

func (r *Request) Get(requestString string, JSONText bool, JSONNative bool) map[string]interface{} {
	
	payload := strings.NewReader(requestString)

	req, _ := http.NewRequest("POST", r.URL, payload)

	if JSONText {
		req.Header.Add("Content-Type", "application/json")
	} else if JSONNative {
		req.Header.Add("Content-Type", "application/json-rpc")
	}
	
	req.Header.Add("Cache-Control", "no-cache")
	req.SetBasicAuth(r.Username, r.Password)

	res, _ := http.DefaultClient.Do(req)
	
	defer res.Body.Close()

	responseBody, _ := ioutil.ReadAll(res.Body)
	
	body := make(map[string]interface{})

	if JSONText {
		var INSAPIResponseJSONText INSAPIResponseJSONText
		err := json.Unmarshal(responseBody, &INSAPIResponseJSONText)
		if err != nil {
			panic(err)
		}

		err = json.Unmarshal([]byte(INSAPIResponseJSONText.INSAPI.Outputs.Output.Body), &body)

		if err != nil {
			panic(err)
		}
	} else if JSONNative {
		var INSAPIResponseJSONNative INSAPIResponseJSONNative
		err := json.Unmarshal(responseBody, &INSAPIResponseJSONNative)
		if err != nil {
			panic(err)
		}

		body = INSAPIResponseJSONNative.Result.Body

	}

	return body
}

func KeyTransform(Key string, JSONText bool, JSONNative bool) string {
	if JSONText {
		Key = "{\n  \"ins_api\": {\n    \"version\": \"1.0\",\n    \"type\": \"cli_show_ascii\",\n    \"chunk\": \"0\",\n    \"sid\": \"sid\",\n    \"input\": \"" + Key + " \",\n    \"output_format\": \"json\"\n  }\n}"
	}
	if JSONNative {
		Key = "[\n  {\n    \"jsonrpc\": \"2.0\",\n    \"method\": \"cli\",\n    \"params\": {\n      \"cmd\": \"" + Key + "\",\n      \"version\": 1\n    },\n    \"id\": 1\n  }\n]"
	}

	return Key
}

func (ig *INSGeneric) Loop(r *Request) {
	for _, INSPath := range(ig.INSPaths){
		INSPath.Key = KeyTransform(INSPath.Key,INSPath.JSONText,INSPath.JSONNative)
		src := r.Get(INSPath.Key,INSPath.JSONText,INSPath.JSONNative)
		for _, path := range(INSPath.Paths){
			src = cu.CopyMap(src)
			worker(src, ig.ESClient, ig.Config.ESIndex, path, ig.Mode, ig.Filter, ig.Enrich, r.Hostname)
		}
	}
}

func (ig *INSGeneric) HostLoop(r *Request) {
	for {
		ig.Loop(r)
		time.Sleep(4 * time.Second)
	}
}

func main() {

	var wg sync.WaitGroup

	ESClient, Config, Inventory, INSPaths, Filter, Enrich := Initialize("config.json","inventory.json")
	INSGeneric := &INSGeneric{Config: Config, INSPaths: INSPaths, ESClient: ESClient, Filter: Filter, Enrich: Enrich, Mode: 1} 

	wg.Add(len(Inventory))

	for _, v := range(Inventory){
		Request := &Request{URL: v.Host.URL, Hostname: v.Host.Hostname, Username: v.Host.Username, Password: v.Host.Password}
		go INSGeneric.HostLoop(Request)
	}

	wg.Wait()
}
