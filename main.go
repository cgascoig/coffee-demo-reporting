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
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
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
	dbName                 = "coffee-demo"
	ordersCollectionName   = "orders"
	accountsCollectionName = "employeeAccounts"
	dbTimeout              = 5 * time.Second

	reportOrderCount = 5
)

type reportingServer struct {
	log *logrus.Logger

	// MongoDB
	mongo *mongo.Client
}

func (rs *reportingServer) reportHandler(w http.ResponseWriter, r *http.Request) {
	type coffeeOrder struct {
		ID         string  `bson:"_id,omitempty" json:"_id,omitempty"`
		CoffeeType string  `bson:"coffeetype" json:"coffeetype"`
		CoffeeQty  int     `bson:"coffeeqty" json:"coffeeqty"`
		EmployeeID string  `bson:"employeeId" json:"employeeId"`
		Amount     float32 `bson:"amount" json:"amount"`
	}
	type employeeAccount struct {
		ID         string  `bson:"_id,omitempty" json:"_id,omitempty"`
		EmployeeID string  `bson:"employeeId" json:"employeeId"`
		Balance    float32 `bson:"balance" json:"balance"`
		Name       string  `bson:"name" json:"name"`
	}
	type report struct {
		TotalSales       int               `json:"totalsales"`
		TotalRevenue     float32           `json:"totalrevenue"`
		RecentSales      []coffeeOrder     `json:"recentsales"`
		EmployeeAccounts []employeeAccount `json:"employeeaccounts"`
	}

	res := report{}

	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	ordersCollection := rs.mongo.Database(dbName).Collection(ordersCollectionName)

	// Find the most recent reportOrderCount orders (i.e. find with sort and limit)
	orderCursor, err := ordersCollection.Find(ctx, nil, findopt.Sort(bson.NewDocument(bson.EC.Int32("_id", -1))), findopt.Limit(reportOrderCount))
	if err != nil {
		rs.log.Error("Error querying database: ", err)
		fmt.Fprintf(w, "[]")
		return
	}

	defer orderCursor.Close(ctx)
	for orderCursor.Next(ctx) {
		order := coffeeOrder{}
		if err := orderCursor.Decode(&order); err == nil {
			res.RecentSales = append(res.RecentSales, order)
		}
	}

	accountsCollection := rs.mongo.Database(dbName).Collection(accountsCollectionName)
	accountCursor, err := accountsCollection.Find(ctx, nil)
	if err != nil {
		rs.log.Error("Error querying database: ", err)
		fmt.Fprintf(w, "[]")
		return
	}

	defer accountCursor.Close(ctx)
	for accountCursor.Next(ctx) {
		account := employeeAccount{}
		if err := accountCursor.Decode(&account); err == nil {
			res.EmployeeAccounts = append(res.EmployeeAccounts, account)
		}
	}

	stageGroup := bson.VC.DocumentFromElements(
		bson.EC.SubDocumentFromElements(
			"$group",
			bson.EC.Int64("_id", 0),
			bson.EC.SubDocumentFromElements(
				"totalSales",
				bson.EC.String("$sum", "$coffeeqty"),
			),
			bson.EC.SubDocumentFromElements(
				"totalRevenue",
				bson.EC.String("$sum", "$amount"),
			),
		),
	)

	totalCursor, err := ordersCollection.Aggregate(ctx, bson.NewArray(stageGroup))
	if err != nil || !totalCursor.Next(ctx) {
		rs.log.Error("Error querying database: ", err)
		fmt.Fprintf(w, "[]")
		return
	}

	defer totalCursor.Close(ctx)
	totalDoc := bson.NewDocument()
	if err := totalCursor.Decode(totalDoc); err == nil {
		res.TotalSales = int(totalDoc.Lookup("totalSales").Int64())
		res.TotalRevenue = float32(totalDoc.Lookup("totalRevenue").Double())
	} else {
		rs.log.Error("Error querying database: ", err)
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
	fmt.Printf("Sending response: %v, JSON: %v\n", res, string(jsResults))
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
