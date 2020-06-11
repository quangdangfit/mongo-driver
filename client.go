package dbs

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"transport/lib/utils/logger"
)

type DBConfig struct {
	MongoDBHosts string
	AuthDatabase string
	AuthUserName string
	AuthPassword string
}

// MongoClient is the wrapper for mongo.Client
type MongoClient struct {
	client *mongo.Client
}

//func Connect(host string, port int, database string, username string, password string) (*mongo.Client, error) {
func (db *MongoClient) Connect(config DBConfig) error {
	connectionURI := "mongodb://"
	if config.AuthUserName != "" && config.AuthPassword != "" {
		connectionURI += fmt.Sprintf("%s:%s@", config.AuthUserName, config.AuthPassword)
	}
	connectionURI += config.MongoDBHosts
	if config.AuthDatabase != "" {
		connectionURI += fmt.Sprintf("/?authSource=%s", config.AuthDatabase)
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(connectionURI))

	if err != nil {
		logger.Error("Cannot connect MongoDB: ", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), OperationTimeOut*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		logger.Error("Cannot connect MongoDB: ", err)
		return err
	}
	db.client = client
	return nil
}

func (db *MongoClient) GetCollection(dbName string, collectionName string) MongoCollection {
	_ = db.client.Database(dbName).Collection(collectionName)
	c := NewMongoCollection()
	return c
}

func (db *MongoClient) CreateIndex(database string, collection string, indexModel mongo.IndexModel) {
	c := db.client.Database(database).Collection(collection)
	opts := options.CreateIndexes().SetMaxTime(OperationTimeOut * time.Second)
	_, err := c.Indexes().CreateOne(context.Background(), indexModel, opts)

	if err != nil {
		logger.Error("[CreateIndex] ERROR: ", err)
	} else {
		logger.Info("[CreateIndex] Successfully create index")
	}
}

func (db *MongoClient) DropIndex(client *mongo.Client, database string, collection string, indexName string) {
	c := client.Database(database).Collection(collection)
	opts := options.DropIndexes().SetMaxTime(OperationTimeOut * time.Second)
	_, err := c.Indexes().DropOne(context.Background(), indexName, opts)
	if err != nil {
		logger.Error("[DropIndex] ERROR: ", err)
	} else {
		logger.Info("[DropIndex] Successfully drop index")
	}
}

func YieldIndexModel() mongo.IndexModel {
	opt := options.Index()
	opt.SetUnique(true)
	index := mongo.IndexModel{
		Keys: bson.M{
			"title": "text",
		},
		Options: opt}
	return index
}
