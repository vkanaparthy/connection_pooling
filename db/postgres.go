package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
)

// Config represents the configuration for connecting to the Postgres database.
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

// Client represents the Postgres client.
type Client struct {
	Conn *pgx.Conn
}

type ClientPool struct {
	mu             *sync.Mutex
	conns          []*Client
	maxConnections int
	channel        chan interface{}
}

func NewClientPool(config Config, maxConns int) (*ClientPool, error) {

	var mu = sync.Mutex{}
	clientPool := &ClientPool{
		mu:             &mu,
		conns:          make([]*Client, 0, maxConns),
		maxConnections: maxConns,
		channel:        make(chan interface{}, maxConns),
	}
	for range maxConns {

		client, err := NewClient(config)
		if err != nil {
			return nil, err
		}

		clientPool.conns = append(clientPool.conns, client)
		clientPool.channel <- nil
	}
	return clientPool, nil
}

func (pool *ClientPool) Acquire() *Client {
	<-pool.channel

	pool.mu.Lock()
	client := pool.conns[0]
	pool.conns = pool.conns[1:]
	pool.mu.Unlock()
	return client
}

func (pool *ClientPool) Release(client *Client) {
	pool.mu.Lock()
	pool.conns = append(pool.conns, client)
	pool.channel <- nil
	pool.mu.Unlock()
}

func (pool *ClientPool) Close() error {
	//fmt.Println("Closing the connection pool")
	for i := range pool.conns {
		err := pool.conns[i].Conn.Close(context.Background())
		if err != nil {
			fmt.Printf("Unable to close the connection")
			return err
		}
	}
	//fmt.Println("Connection pool drained!!")
	return nil
}

// Create a Postgres client
func NewClient(config Config) (*Client, error) {
	connConfig, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s", config.Username, config.Password, config.Host, config.Port, config.Database))
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return nil, err
	}

	return &Client{
		Conn: conn,
	}, nil
}

func (c *Client) Close() error {
	return c.Conn.Close(context.Background())
}
