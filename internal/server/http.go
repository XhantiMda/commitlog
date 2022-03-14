package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
)

func NewHttpServer(address string) *http.Server {
	httpServer := newHttpServer()

	router := mux.NewRouter()

	router.HandleFunc("/", httpServer.HandleProduce).Methods("POST")
	router.HandleFunc("/", httpServer.HandleConsume).Methods("GET")

	return &http.Server{
		Addr:    address,
		Handler: router,
	}
}

func newHttpServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func (server *httpServer) HandleProduce(responseWriter http.ResponseWriter, request *http.Request) {

	var produceRequest ProduceRequest

	//parse the request body to a concrete ProduceRequest instance
	error := json.NewDecoder(request.Body).Decode(&produceRequest)

	//if failed to parse, return bad request
	if error != nil {
		http.Error(responseWriter, error.Error(), http.StatusBadRequest)
		return
	}

	//append the record provided on the request to the log
	offset, error := server.Log.Append(produceRequest.Record)

	//if failed to append record, return bad request
	if error != nil {
		http.Error(responseWriter, error.Error(), http.StatusBadRequest)
		return
	}

	produceResponse := ProduceResponse{Offset: offset}

	error = json.NewEncoder(responseWriter).Encode(produceResponse)

	if error != nil {
		http.Error(responseWriter, error.Error(), http.StatusBadRequest)
		return
	}
}

func (server *httpServer) HandleConsume(responseWritter http.ResponseWriter, request *http.Request) {

	var consumeRequest ConsumeRequest

	error := json.NewDecoder(request.Body).Decode(&consumeRequest)

	if error != nil {
		http.Error(responseWritter, error.Error(), http.StatusBadRequest)
		return
	}

	record, error := server.Log.Read(consumeRequest.Offset)

	if error == ErrOffsetNotFound {
		http.Error(responseWritter, error.Error(), http.StatusNotFound)
		return
	}

	if error != nil {
		http.Error(responseWritter, error.Error(), http.StatusInternalServerError)
		return
	}

	consumeResponse := ConsumeResponse{Record: record}

	error = json.NewEncoder(responseWritter).Encode(consumeResponse)

	if error != nil {
		http.Error(responseWritter, error.Error(), http.StatusBadRequest)
		return
	}
}

type httpServer struct {
	Log *Log
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}
