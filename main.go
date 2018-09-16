package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/mongodb/mongo-go-driver/mongo"
)

var (
	verbose         bool
	tls             bool
	certFilename    string
	certKeyFilename string
	listenAddr      string
	mongoConnString string
)

const (
	dbName               = "coffee-demo"
	ordersCollectionName = "orders"
	dbTimeout            = 5 * time.Second
)

type reportingServer struct {
	log *logrus.Logger

	// MongoDB
	mongo *mongo.Client
}

func (rs *reportingServer) reportHandler(w http.ResponseWriter, r *http.Request) {
	type coffeeOrder struct {
		CoffeeType string
		CoffeeQty  int
	}
	type report struct {
		TotalSales  int
		RecentSales []coffeeOrder
	}

	res := report{}

	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	ordersCollection := rs.mongo.Database(dbName).Collection(ordersCollectionName)

	orderCursor, err := ordersCollection.Find(ctx, nil)
	if err != nil {
		rs.log.Error("Error querying database: ", err)
		fmt.Fprintf(w, "[]")
		return
	}

	defer orderCursor.Close(ctx)
	for orderCursor.Next(ctx) {
		orderDoc := bson.NewDocument()
		if err := orderCursor.Decode(orderDoc); err == nil {
			rs.log.Info("Order: ", orderDoc)
			order := coffeeOrder{
				CoffeeType: orderDoc.Lookup("coffeetype").StringValue(),
				CoffeeQty:  int(orderDoc.Lookup("coffeeqty").Int64()),
			}
			res.RecentSales = append(res.RecentSales, order)
		}
	}

	jsResults, err := json.Marshal(res)
	if err != nil {
		rs.log.Error("Error querying results from mongodb: ", err)
		fmt.Fprintf(w, "[]")
		return
	}

	if string(jsResults) == "null" {
		jsResults = []byte("[]")
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Printf("Sending response: %v|%v\n", res, string(jsResults))
	fmt.Fprint(w, string(jsResults))
}

func (rs *reportingServer) loggingHandler(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		rs.log.WithFields(logrus.Fields{"Method": r.Method, "URI": r.RequestURI}).Info("Handling request")
		handler(w, r)
		rs.log.Debug("Finished handling request")
	}
}

func (rs *reportingServer) getRouter() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/report", rs.loggingHandler(rs.reportHandler)).Methods("GET")

	return r
}

func newReportingServer(log *logrus.Logger) *reportingServer {
	rs := reportingServer{
		log: log,
	}

	if mongoConnString != "" {
		db, err := mongo.NewClient(mongoConnString)
		if err != nil {
			log.Error("Error creating mongodb connection: ", err)
			return nil
		}
		err = db.Connect(context.TODO())
		if err != nil {
			log.Error("Error creating mongodb connection: ", err)
			return nil
		}

		log.Info("Created mongodb connection for ", mongoConnString)

		rs.mongo = db
	}

	return &rs
}

func run(log *logrus.Logger) {
	cs := newReportingServer(log)
	r := cs.getRouter()

	if tls {
		log.Info("Starting HTTPS server on ", listenAddr)
		log.Error("HTTP server shutdown: ", http.ListenAndServeTLS(listenAddr, certFilename, certKeyFilename, r))
	} else {
		log.Info("Starting HTTP server on ", listenAddr)
		log.Error("HTTP server shutdown: ", http.ListenAndServe(listenAddr, r))
	}

}

func main() {
	flag.Parse()

	log := logrus.New()
	if verbose {
		log.Level = logrus.DebugLevel
		log.Debug("Logging level set to debug")
	}
	run(log)
}

func init() {
	flag.BoolVar(&verbose, "verbose", false, "Verbose logging")
	flag.StringVar(&listenAddr, "addr", ":5000", "Address to listen on")
	flag.StringVar(&mongoConnString, "mongo", "mongodb://localhost:27017", "Connection string for mondodb server")

	flag.BoolVar(&tls, "tls", false, "Enable TLS")
	flag.StringVar(&certFilename, "cert", "", "Filename for certificate file (e.g. cert.pem)")
	flag.StringVar(&certKeyFilename, "certkey", "", "Filename for certificate private key file (e.g. key.pem)")
}
