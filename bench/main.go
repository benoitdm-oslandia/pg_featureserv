package main

/*
 Copyright 2022 Crunchy Data Solutions, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 Date     : Octobre 2022
 Authors  : Jean-philippe Bazonnais (jean-philippe dot bazonnais at ign dot fr)
*/

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/pborman/getopt/v2"
)

// workflow avec benchstat
// > on lance les benchmarks que l'on compare avec les résultats faits sur cruncyData::master
// > on pousse un rapport (html ou texte) pour garder une trace
// > sur les benchmarks, on n'oublie pas d'exposer la variable globale PGFS_CACHE pour comparer
// les temps de traitements avec ou sans cache.

var m_version string
var m_name string

var m_flagHelp bool
var m_flagVersion bool
var m_flagConfigFilename string
var m_flagOutputDir string
var m_flagFormat string

var m_Html Html

type m_Config struct {
	Cmd        string        `json:"cmd"`
	Host       string        `json:"host"`
	Port       int           `json:"port"`
	Report     string        `json:"reportdir"`
	Ref        string        `json:"refdir"`
	Env        []string      `json:"env,omitempty"`
	Benchmarks []m_Benchmark `json:"benchmarks"`
}

type m_Benchmark struct {
	Name  string   `json:"name"`
	Ref   string   `json:"ref,omitempty"`
	Pkg   string   `json:"pkg"`
	Env   []string `json:"env,omitempty"`
	Actif int      `json:"actif"`
}

//--------------------------------------------------//
// HTML Class

type Html struct {
	Tag    string
	Class  string
	Header bool
	Footer bool
}

func (Html) header() string {
	head := `<html>
	<head>
	<meta charset="utf-8">
	<title>Benchmarcks Report</title>
	<style>
	.benchmark-out > pre:has(meta) { margin-top: -100px; }
	.benchmark-cmd {text-align: left; color: #c00;}
	.benchmark-env {text-align: left;}
	.benchmark-name {font-weight: bold;}
	.benchmark-out {text-align: left; border-top: 1px solid #ccc; border-bottom: 3px solid #666;}
	.benchmark-title {font-weight: bold;}
	.benchmark {}
	</style>
	</head>
	<body>`
	return head
}

func (Html) footer() string {
	foot := `</body>
	</html>`
	return foot
}

func (Html) convert(message string) string {
	if m_Html.Footer {
		return m_Html.footer()
	}
	if m_Html.Header {
		return m_Html.header()
	}
	// tag by default
	if m_Html.Tag == "" {
		m_Html.Tag = "p"
	}
	// class by default
	if m_Html.Class == "" {
		m_Html.Class = "benchmark"
	}
	// special
	if m_Html.Class == "benchmark-out" {
		return fmt.Sprintf("<%s class=\"%s\"><pre>%s</pre></%s>", m_Html.Tag, m_Html.Class, message, m_Html.Tag)
	}
	return fmt.Sprintf("<%s class=\"%s\">%s</%s>", m_Html.Tag, m_Html.Class, message, m_Html.Tag)
}

//--------------------------------------------------//

func init() {
	initCommnandOptions()
}

func initCommnandOptions() {
	getopt.FlagLong(&m_flagHelp, "help", '?', "Show command usage")
	getopt.FlagLong(&m_flagConfigFilename, "config", 'c', "", "config file name")
	getopt.FlagLong(&m_flagOutputDir, "output", 'o', "", "result directory output")
	getopt.FlagLong(&m_flagFormat, "format", 'f', "", "Output format : html, log or stdout")
	getopt.FlagLong(&m_flagVersion, "version", 'v', "Output the version information")
}

func runProcess(process string, args []string, env []string, output *os.File) []byte {
	cmd := exec.Command(process)
	cmd.Args = append(cmd.Args, args...)
	cmd.Env = append(cmd.Env, env...)

	var b bytes.Buffer
	cmd.Stdout = &b
	cmd.Stderr = os.Stderr

	logEntry(output, fmt.Sprintf("\n> CMD : %+v\n", cmd), "cmd")
	logEntry(output, fmt.Sprintf("\n> ENV : %+v\n\n", env), "env")

	errRun := cmd.Run()
	if errRun != nil {
		log.Fatalf("RUN : %s", errRun)
		os.Exit(1)
	}

	logEntry(output, b.String(), "out")

	return b.Bytes()
}

func logEntry(fd *os.File, message string, htmlcode string) {
	if m_flagFormat == "html" {
		switch htmlcode {
		case "out":
			m_Html = Html{
				"div",
				"benchmark-out",
				false,
				false,
			}
			break
		case "env":
			m_Html = Html{
				"div",
				"benchmark-env",
				false,
				false,
			}
			break
		case "cmd":
			m_Html = Html{
				"p",
				"benchmark-cmd",
				false,
				false,
			}
			break
		case "name":
			m_Html = Html{
				"p",
				"benchmark-name",
				false,
				false,
			}
			break
		case "title":
			m_Html = Html{
				"p",
				"benchmark-title",
				false,
				false,
			}
			break
		case "header":
			m_Html = Html{
				Header: true,
				Footer: false,
			}
			break
		case "footer":
			m_Html = Html{
				Header: false,
				Footer: true,
			}
			break
		}
		message = m_Html.convert(message)
	}

	if message != "" {
		_, err := fd.WriteString(message)
		if err != nil {
			log.Fatalf("WRITEMSG : %s", err)
			os.Exit(1)
		}
	}
}

//--------------------------------------------------//
// main program

func main() {

	getopt.Parse()

	if m_flagHelp {
		getopt.Usage()
		os.Exit(1)
	}

	if m_flagVersion {
		fmt.Printf("%s %s\n", m_name, m_version)
		os.Exit(1)
	}

	// format by default
	outputFormatExt := "log"
	if m_flagFormat == "" {
		m_flagFormat = "log"
	}

	if m_flagFormat == "html" {
		outputFormatExt = "html"
	}

	// config by default
	configFilename := "data.json"

	// custom config
	if m_flagConfigFilename != "" {
		configFilename = m_flagConfigFilename
	}

	// read config : json
	file, errRead := ioutil.ReadFile(configFilename)
	if errRead != nil {
		log.Fatalf("READCFG : %s", errRead)
		os.Exit(1)
	}

	// struct
	data := m_Config{}
	errJson := json.Unmarshal([]byte(file), &data)
	if errJson != nil {
		log.Fatalf("READJSON : %s", errJson)
		os.Exit(1)
	}

	// report file
	currentTime := time.Now()

	var logFilePath string
	if m_flagFormat != "stdout" {
		logFilePath = fmt.Sprintf("%s/bench-%s.%s", data.Report, currentTime.Format("20060102-150405"), outputFormatExt)
		fmt.Printf("Results are saved in \t: %s\n", logFilePath)
	}

	var fd *os.File
	if m_flagFormat != "stdout" {
		var errCreate error
		fd, errCreate = os.Create(logFilePath)
		if errCreate != nil {
			log.Fatalf("CREATELOG : %s", errCreate)
			os.Exit(1)
		}
		defer fd.Close()
	} else {
		fd = os.Stdout
	}

	logEntry(fd, "", "header")

	// search binary
	execPath, err := exec.LookPath(fmt.Sprintf("%s/bin/%s", os.Getenv("GOPATH"), data.Cmd))
	if err != nil {
		log.Fatalf("EXECPATH : %s", err)
		os.Exit(1)
	}

	for i := 0; i < len(data.Benchmarks); i++ {
		benchmark := data.Benchmarks[i]
		if benchmark.Actif == 1 {
			// create tmp file
			tmp, errTmp := os.Create(fmt.Sprintf("%s/benchmark.tmp", data.Report))
			if errTmp != nil {
				log.Fatalf("CREATETMP : %s", errTmp)
				os.Exit(1)
			}
			defer tmp.Close()

			logEntry(fd, fmt.Sprintf("Scenario \"#%d\":\n", i), "title")
			logEntry(fd, fmt.Sprintf("\n> Run \"%s\"\n", benchmark.Name), "name")

			// run benchmark
			process := "go"
			args := []string{}
			args = append(args, "test")
			args = append(args, "-count=1") // remove cache ?
			args = append(args, "-benchmem")
			args = append(args, "-run")
			args = append(args, "^$")
			args = append(args, "-bench")
			args = append(args, fmt.Sprintf("^%s$", benchmark.Name))
			args = append(args, benchmark.Pkg)

			env := []string{}
			env = append(env, data.Env...)
			env = append(env, benchmark.Env...)
			env = append(env, fmt.Sprintf("GOCACHE=%s/.cache/go-build", os.Getenv("HOME")))
			env = append(env, fmt.Sprintf("GOPATH=%s/go", os.Getenv("HOME")))
			env = append(env, "PATH=$PATH:$GOPATH/bin")

			b := runProcess(process, args, env, fd)

			// write tmp file
			_, err := tmp.Write(b)
			if err != nil {
				log.Fatalf("WRITETMP : %s", err)
				os.Exit(1)
			}

			// is there a reference for the comparison test ?
			if benchmark.Ref != "" {
				// run benchstat
				process := execPath
				args := []string{}
				args = append(args, "-delta-test")
				args = append(args, "none")
				if m_flagFormat == "html" {
					args = append(args, "-html")
				}
				args = append(args, fmt.Sprintf("%s/benchmark.tmp", data.Report))
				args = append(args, fmt.Sprintf("%s/%s", data.Ref, benchmark.Ref))

				env := []string{}
				env = append(env, fmt.Sprintf("GOCACHE=%s/.cache/go-build", os.Getenv("HOME")))
				env = append(env, fmt.Sprintf("GOPATH=%s/go", os.Getenv("HOME")))
				env = append(env, "PATH=$PATH:$GOPATH/bin")

				runProcess(process, args, env, fd)
			}

			logEntry(fd, "\n", "out")
		}
	}

	logEntry(fd, "", "footer")
}
