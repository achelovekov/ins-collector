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

type INSAPIResponseJsonText struct {
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

type INSAPIResponseJsonNative struct {
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
	cu.Config
	InventoryFile string  `json:"InventoryFile"`
}

type KeysDefinition []KeyDefinition
type KeyDefinition struct {
	cu.KeyDefinition
	JsonNative bool   `json:"jsonNative"`
	JsonText   bool   `json:"jsonText"`
}


type KeysMap map[string]KeyData
type KeyData struct {
	Paths Paths
	PathOptions PathOptions
}
type Paths []cu.Path

type PathOptions struct {
	JsonNative bool
	JsonText bool
}


type MetaData struct {
	Config   Config
	KeysMap KeysMap
	ESClient cu.ESClient
	Filter   cu.Filter
	Enrich   cu.Enrich
	Mode     int
}

type Request struct {
	URL string
	Hostname string
	RequestString string
	Username string
	Password string	 
}

func LoadKeysMap(fileName string) KeysMap {

	var KeysDefinition KeysDefinition
	KeysMap := make(KeysMap)

	KeysDefinitionFile, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
	}
	defer KeysDefinitionFile.Close()

	KeysDefinitionFileBytes, err := ioutil.ReadAll(KeysDefinitionFile)
	if err != nil {
		fmt.Println(err)
	}

	err = json.Unmarshal(KeysDefinitionFileBytes, &KeysDefinition)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range KeysDefinition {

		KeyData := KeyData{PathOptions: PathOptions{
			JsonNative: v.JsonNative, 
			JsonText: v.JsonText,
			},
		}

		var Paths Paths

		for _, v := range v.Paths {
			pathFile, err := os.Open(v.Path)
			if err != nil {
				fmt.Println(err)
			}
			defer pathFile.Close()

			pathFileBytes, _ := ioutil.ReadAll(pathFile)

			var Path cu.Path
			err = json.Unmarshal(pathFileBytes, &Path)
			if err != nil {
				fmt.Println(err)
			}
			Paths = append(Paths, Path)
		}

		KeyData.Paths = Paths
		KeysMap[v.Key] = KeyData
	}

	return KeysMap
}

func Initialize(configFile string) (cu.ESClient, Config, Inventory, KeysMap, cu.Filter, cu.Enrich) {

	var Config Config
	var Inventory Inventory
	var KeysMap KeysMap
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

	InventoryFile, err := os.Open(Config.InventoryFile)
	if err != nil {
		fmt.Println(err)
	}
	defer InventoryFile.Close()

	InventoryFileBytes, _ := ioutil.ReadAll(InventoryFile)

	err = json.Unmarshal(InventoryFileBytes, &Inventory)
	if err != nil {
		fmt.Println(err)
	}

	KeysMap = LoadKeysMap(Config.KeysDefinitionFile)

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

	return ESClient, Config, Inventory, KeysMap, Filter, Enrich
}

func (r *Request) Get(requestString string, JsonText bool, JsonNative bool) map[string]interface{} {
	
	payload := strings.NewReader(requestString)

	req, _ := http.NewRequest("POST", r.URL, payload)

	if JsonText {
		req.Header.Add("Content-Type", "application/json")
	} else if JsonNative {
		req.Header.Add("Content-Type", "application/json-rpc")
	}
	
	req.Header.Add("Cache-Control", "no-cache")
	req.SetBasicAuth(r.Username, r.Password)

	res, _ := http.DefaultClient.Do(req)
	
	defer res.Body.Close()

	responseBody, _ := ioutil.ReadAll(res.Body)
	
	body := make(map[string]interface{})

	if JsonText {
		var INSAPIResponseJsonText INSAPIResponseJsonText
		err := json.Unmarshal(responseBody, &INSAPIResponseJsonText)
		if err != nil {
			panic(err)
		}

		err = json.Unmarshal([]byte(INSAPIResponseJsonText.INSAPI.Outputs.Output.Body), &body)

		if err != nil {
			panic(err)
		}
	} else if JsonNative {
		var INSAPIResponseJsonNative INSAPIResponseJsonNative
		err := json.Unmarshal(responseBody, &INSAPIResponseJsonNative)
		if err != nil {
			panic(err)
		}

		body = INSAPIResponseJsonNative.Result.Body

	}

	return body
}

func KeyTransform(Key string, JsonText bool, JsonNative bool) string {
	if JsonText {
		Key = "{\n  \"ins_api\": {\n    \"version\": \"1.0\",\n    \"type\": \"cli_show_ascii\",\n    \"chunk\": \"0\",\n    \"sid\": \"sid\",\n    \"input\": \"" + Key + " \",\n    \"output_format\": \"json\"\n  }\n}"
	}
	if JsonNative {
		Key = "[\n  {\n    \"jsonrpc\": \"2.0\",\n    \"method\": \"cli\",\n    \"params\": {\n      \"cmd\": \"" + Key + "\",\n      \"version\": 1\n    },\n    \"id\": 1\n  }\n]"
	}

	return Key
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

func Loop(md *MetaData, r *Request) {
	for k, v := range(md.KeysMap){
		keyTransformed := KeyTransform(k, v.PathOptions.JsonText, v.PathOptions.JsonNative)
		src := r.Get(keyTransformed, v.PathOptions.JsonText, v.PathOptions.JsonNative)
		for _, path := range(v.Paths){
			src = cu.CopyMap(src)
			worker(src, md.ESClient, md.Config.ESIndex, path, md.Mode, md.Filter, md.Enrich, r.Hostname)
		}
	}
}

func HostLoop(md *MetaData, r *Request) {
	for {
		Loop(md, r)
		time.Sleep(4 * time.Second)
	}
}

func main() {

	var wg sync.WaitGroup

	ESClient, Config, Inventory, KeysMap, Filter, Enrich := Initialize("config.json")
	MetaData := &MetaData{Config: Config, KeysMap: KeysMap, ESClient: ESClient, Filter: Filter, Enrich: Enrich, Mode: 1} 

	wg.Add(len(Inventory))

	for _, v := range(Inventory){
		Request := &Request{URL: v.Host.URL, Hostname: v.Host.Hostname, Username: v.Host.Username, Password: v.Host.Password}
		go HostLoop(MetaData, Request)
	}

	wg.Wait()
}
