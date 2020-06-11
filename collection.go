package dbs

import (
	"context"
	"os"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

const OperationTimeOut = 10

type MongoCollection interface {
	FindOne(filter interface{}, result interface{}) error
	FindMany(filter interface{}, result interface{}) error
	InsertOne(payload interface{})
	InsertMany(payload []interface{})
	UpdateOne(filter interface{}, payload interface{})
	DeleteOne(filter interface{}) error
	DeleteMany(filter interface{}) error
}

// MongoCollection is the wrapper for mongo.Collection
type mongoCollection struct {
	collection *mongo.Collection
}

func NewMongoCollection() MongoCollection {
	return &mongoCollection{}
}

func (c *mongoCollection) FindOne(filter interface{}, result interface{}) error {
	err := c.collection.FindOne(context.TODO(), filter).Decode(result)
	if err != nil {
		return err
	}

	return nil
}

func (c *mongoCollection) FindMany(filter interface{}, result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("[FindAll] Result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()

	// Passing bson.D{{}} as the filter matches all documents in the collection
	cur, err := c.collection.Find(context.TODO(), filter)
	if err != nil {
	}

	i := 0
	// Finding multiple documents returns a cursor
	// Iterating through the cursor allows us to decode documents one at a time
	for cur.Next(context.TODO()) {
		// create a value into which the single document can be decoded
		elemp := reflect.New(elemt)
		err := cur.Decode(elemp.Interface())
		if err != nil {
			return err
		}

		slicev = reflect.Append(slicev, elemp.Elem())
		slicev = slicev.Slice(0, slicev.Cap())

		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))

	if err := cur.Err(); err != nil {
		return err
	}

	// Close the cursor once finished
	cur.Close(context.TODO())

	return nil
}

func (c *mongoCollection) InsertOne(payload interface{}) {
	ctx, _ := context.WithTimeout(context.Background(), OperationTimeOut*time.Second)

	// InsertOne() method Returns mongo.InsertOneResult
	_, err := c.collection.InsertOne(ctx, payload)
	if err != nil {
		os.Exit(1) // safely exit script on error
	} else {
	}
}

func (c *mongoCollection) InsertMany(payload []interface{}) {
	ctx, _ := context.WithTimeout(context.Background(), OperationTimeOut*time.Second)

	// InsertMany() method Returns mongo.InsertManyResult
	_, err := c.collection.InsertMany(ctx, payload)
	if err != nil {
		os.Exit(1) // safely exit script on error
	} else {
	}
}

func (c *mongoCollection) UpdateOne(filter interface{}, payload interface{}) {
	_, err := c.collection.UpdateOne(context.TODO(), filter, payload)
	if err != nil {
	}
}

func (c *mongoCollection) UpdateMany(filter interface{}, payload interface{}) {
	_, err := c.collection.UpdateMany(context.TODO(), filter, payload)
	if err != nil {
	}

}

// DeleteOne delete single document that match the bson filter
func (c *mongoCollection) DeleteOne(filter interface{}) error {
	ctx, _ := context.WithTimeout(context.Background(), OperationTimeOut*time.Second)

	_, err := c.collection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}
	return nil
}

// DeleteMany delete all documents that match the bson filter
func (c *mongoCollection) DeleteMany(filter interface{}) error {
	ctx, _ := context.WithTimeout(context.Background(), OperationTimeOut*time.Second)

	_, err := c.collection.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}
	return nil
}
