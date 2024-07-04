package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vkanaparthy/connection_pooling/db"
)

const (
	query = "SELECT pg_sleep(0.01);"
)

func main() {

	config := db.Config{
		Host:     "localhost",
		Port:     5432,
		Username: "admin",
		Password: "admin_password",
		Database: "test_conn_pool_db",
	}

	// Benchmark the time taken to execute queries with and without connection pooling
	//BenchmarkNonPooledConnection(config, 10)
	//BenchmarkNonPooledConnection(config, 100)
	//BenchmarkNonPooledConnection(config, 1000)
	//BenchmarkNonPooledConnection(config, 10000)

	//BenchmarkPooledConnection(config, 10, 10)
	//BenchmarkPooledConnection(config, 10, 100)
	BenchmarkPooledConnection(config, 10, 1000)
	//BenchmarkPooledConnection(config, 10, 10000)
	BenchmarkPooledConnection(config, 100, 1000)

}

func BenchmarkPooledConnection(config db.Config, maxConnections int, noOfQueries int) {

	clientPool, err := GetClientPool(config, maxConnections)
	if err != nil {
		fmt.Printf("Unable to close the connection")

	}
	startTime := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < noOfQueries; i++ {
		wg.Add(1)
		go func() {

			defer wg.Done()

			// acquire the client from pool
			client := clientPool.Acquire()
			// execute query
			_, err := client.Conn.Exec(context.Background(), query)
			if err != nil {
				log.Panic("Error executing query:", err)
			}
			// releaes the client back to pool
			clientPool.Release(client)

		}()
	}

	wg.Wait()
	fmt.Printf("Time taken to execute queries with connection pooling: pool size: %d, queries: %d time: %s\n", maxConnections, noOfQueries, time.Since(startTime))
	error := clientPool.Close()
	if error != nil {
		fmt.Println("Error closing the connection.")
	}
}

func GetClientPool(config db.Config, maxConnections int) (*db.ClientPool, error) {
	pool, err := db.NewClientPool(config, maxConnections)
	if err != nil {
		fmt.Println("Error creating connection pool:", err)
		return nil, err
	}

	return pool, nil
}

func BenchmarkNonPooledConnection(config db.Config, noOfQueries int) {

	startTime := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < noOfQueries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := db.NewClient(config)
			if err != nil {
				log.Panic("Error connecting to Postgres:", err)
			}

			defer client.Close()

			_, err = client.Conn.Exec(context.Background(), query)
			if err != nil {
				log.Panic("Error executing query:", err)
			}
		}()

		wg.Wait()
	}

	fmt.Printf("Time taken to execute queries without connection pooling: queries: %d time: %s", noOfQueries, time.Since(startTime))
}
