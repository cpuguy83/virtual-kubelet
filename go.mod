module github.com/virtual-kubelet/virtual-kubelet

go 1.12

require (
	contrib.go.opencensus.io/exporter/jaeger v0.1.0
	contrib.go.opencensus.io/exporter/ocagent v0.5.0
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/spdystream v0.0.0-20181023171402-6480d4af844c // indirect
	github.com/elazarl/goproxy v0.0.0-20190630181448-f1e96bc0f4c5 // indirect
	github.com/elazarl/goproxy/ext v0.0.0-20190630181448-f1e96bc0f4c5 // indirect
	github.com/gogo/protobuf v1.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/google/go-cmp v0.3.0
	github.com/gorilla/mux v1.7.3
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/json-iterator/go v1.1.6 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	go.opencensus.io v0.22.0
	golang.org/x/oauth2 v0.0.0-20190402181905-9f3314589c9a // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	google.golang.org/appengine v1.5.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.0.0-20190627205229-acea843d18eb
	k8s.io/apimachinery v0.0.0-20190629125103-05b5762916b3
	k8s.io/apiserver v0.0.0-20190702125801-bf12d466f628 // indirect
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/klog v0.3.3
	k8s.io/kubernetes v1.13.7
	k8s.io/utils v0.0.0-20190607212802-c55fbcfc754a // indirect
)

replace k8s.io/api => k8s.io/api v0.0.0-20190222213804-5cb15d344471

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190221213512-86fb29eff628
