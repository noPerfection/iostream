module github.com/noPerfection/iostream

go 1.22

require (
	github.com/noPerfection/datatype v0.0.0
	github.com/noPerfection/log v0.0.0
	github.com/noPerfection/protocol/handler v0.0.0
	github.com/noPerfection/protocol/message v0.0.0
	github.com/pebbe/zmq4 v1.4.0
	github.com/stretchr/testify v1.8.4
)

replace (
	github.com/noPerfection/datatype => ../datatype
	github.com/noPerfection/log => ../log
	github.com/noPerfection/protocol/client => ../protocol/client
	github.com/noPerfection/protocol/handler => ../protocol/handler
	github.com/noPerfection/protocol/message => ../protocol/message
)

require (
	github.com/ahmetson/mushroom v0.0.0-20260707102258-ccc67a16e74a // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/charmbracelet/lipgloss v0.8.0 // indirect
	github.com/charmbracelet/log v0.2.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/muesli/clusters v0.0.0-20200529215643-2700303c1762 // indirect
	github.com/muesli/gamut v0.3.1 // indirect
	github.com/muesli/kmeans v0.3.1 // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.15.2 // indirect
	github.com/noPerfection/protocol/client v0.0.0-20260708100121-590bd988188f // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	golang.org/x/sys v0.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
