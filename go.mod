module github.com/virtual-kubelet/virtual-kubelet

go 1.12

require (
	contrib.go.opencensus.io/exporter/ocagent v0.4.12
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/spdystream v0.0.0-20170912183627-bc6354cbbc29 // indirect
	github.com/elazarl/goproxy v1.2.1 // indirect
	github.com/evanphx/json-patch v4.1.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/go-cmp v0.6.0
	github.com/google/gofuzz v1.0.0 // indirect
	github.com/googleapis/gnostic v0.1.0 // indirect
	github.com/gorilla/mux v1.6.2
	github.com/gregjones/httpcache v0.0.0-20180305231024-9cad4c3443a7 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/json-iterator/go v1.1.6 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.4.1
	github.com/spf13/cobra v0.0.2
	github.com/spf13/pflag v1.0.3
	go.opencensus.io v0.24.0
	google.golang.org/grpc v1.56.3 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.15.10
	k8s.io/apimachinery v0.15.10
	k8s.io/apiserver v0.15.10 // indirect
	k8s.io/client-go v10.0.0+incompatible
	k8s.io/klog v0.3.1
	k8s.io/kube-openapi v0.0.0-20190510232812-a01b7d5d6c22 // indirect
	k8s.io/kubernetes v1.13.7
)

replace k8s.io/api => k8s.io/api v0.0.0-20190222213804-5cb15d344471

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190221213512-86fb29eff628
